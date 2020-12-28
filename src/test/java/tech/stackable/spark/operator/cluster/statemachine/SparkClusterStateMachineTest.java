package tech.stackable.spark.operator.cluster.statemachine;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;


import java.util.List;

import common.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import tech.stackable.spark.operator.cluster.SparkCluster;
import tech.stackable.spark.operator.cluster.SparkClusterController;
import tech.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine.ClusterState;
import tech.stackable.spark.operator.common.state.PodState;
import tech.stackable.spark.operator.common.type.SparkOperatorConfig;

@TestInstance(Lifecycle.PER_CLASS)
public class SparkClusterStateMachineTest {

  private KubernetesServer server;
  private KubernetesClient client;

  private SparkClusterController controller;

  @BeforeEach
  public void init() {
    server = new KubernetesServer(true, true);
    server.before();

    client = server.getClient();
    controller = new SparkClusterController(client, Util.CLUSTER_CRD_PATH, Util.RESYNC_CYCLE);
    controller.init();
  }

  @AfterEach
  public void cleanUp() {
    client.close();
    server.after();
  }

  @Test
  public void testNonThreadedStateTransitions() {
    // initial state
    assertEquals(ClusterState.READY, controller.getClusterStateMachine().getState());
    // load spark-cluster-example.yaml
    SparkCluster cluster =
      controller.getCrdClient()
        .load(Thread.currentThread().getContextClassLoader().getResourceAsStream(Util.CLUSTER_EXAMPLE_PATH))
        .create();
    cluster.getMetadata().setUid("123456789");
    cluster.getMetadata().setNamespace(client.getNamespace());

    cluster = controller.getCrdClient().create(cluster);

    controller.process(cluster);

    // assume state INITIAL after cluster first arrived
    assertEquals(ClusterState.INITIAL, getState(controller));
    // check if status set, image set
    cluster = controller.getCrdClient().list().getItems().get(0);
    assertNotNull(cluster.getStatus());
    assertNotNull(cluster.getStatus().getImage());
    // check if configmaps deleted
    assertEquals(0, client.configMaps().list().getItems().size());
    // run create master
    controller.process(cluster);

    // check if state is create master
    assertEquals(ClusterState.CREATE_MASTER, getState(controller));
    // master pods created?
    List<Pod> createdMasterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
    assertEquals(cluster.getSpec().getMaster().getInstances(), createdMasterPods.size());

    // state should be create master until hostname arrives
    controller.process(cluster);
    assertEquals(ClusterState.CREATE_MASTER, getState(controller));

    // set node name
    Util.setNodeName(client, createdMasterPods, cluster);

    // when nodenames set -> wait for master hostname
    controller.process(cluster);
    assertEquals(ClusterState.WAIT_MASTER_HOST_NAME, getState(controller));
    // check if config map created
    List<ConfigMap> masterConfigMaps = controller.getConfigMaps(createdMasterPods, cluster);
    assertEquals(cluster.getSpec().getMaster().getInstances(), masterConfigMaps.size());

    // state should be wait master running
    controller.process(cluster);
    assertEquals(ClusterState.WAIT_MASTER_RUNNING, getState(controller));

    // set pods for running ...
    Util.setStatusRunning(client, createdMasterPods);
    // master pods running -> state should be create worker
    controller.process(cluster);
    assertEquals(ClusterState.CREATE_WORKER, getState(controller));
    // worker nodes created?
    List<Pod> createdWorkerPods = controller.getPodsByNode(cluster, cluster.getSpec().getWorker());
    assertEquals(cluster.getSpec().getWorker().getInstances(), createdWorkerPods.size());

    controller.process(cluster);
    assertEquals(ClusterState.CREATE_WORKER, getState(controller));
    // set worker nodenames
    Util.setNodeName(client, createdWorkerPods, cluster);

    // wait for worker host name
    controller.process(cluster);
    assertEquals(ClusterState.WAIT_WORKER_HOST_NAME, getState(controller));

    // wait for workers running
    controller.process(cluster);
    assertEquals(ClusterState.WAIT_WORKER_RUNNING, getState(controller));
    // set workers to running
    Util.setStatusRunning(client, createdWorkerPods);

    // workers running -> READY
    controller.process(cluster);
    assertEquals(ClusterState.READY, getState(controller));

    // delete worker and check for reconcile
    controller.deletePods(cluster, cluster.getSpec().getWorker());
    controller.process(cluster);
    assertEquals(ClusterState.CREATE_WORKER, getState(controller));
  }

  private static ClusterState getState(SparkClusterController controller) {
    return controller.getClusterStateMachine().getState();
  }
}
