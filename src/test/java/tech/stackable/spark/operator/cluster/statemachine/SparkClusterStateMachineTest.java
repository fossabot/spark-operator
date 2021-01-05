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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.SparkClusterController;
import tech.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine.ClusterState;
import tech.stackable.spark.operator.cluster.versioned.SparkVersionedClusterController;
import tech.stackable.spark.operator.cluster.versioned.SparkVersionedClusterControllerFactory;

@TestInstance(Lifecycle.PER_CLASS)
public class SparkClusterStateMachineTest {

  private KubernetesServer server;
  private KubernetesClient client;

  private SparkClusterController clusterController;
  private SparkVersionedClusterController controller;

  @BeforeEach
  public void init() {
    server = new KubernetesServer(true, true);
    server.before();

    client = server.getClient();
    clusterController = new SparkClusterController(client, Util.CLUSTER_CRD_PATH, Util.RESYNC_CYCLE);
    clusterController.init();

    controller = SparkVersionedClusterControllerFactory
        .getSparkVersionedController(Util.IMAGE_VERSION_3_0_1, client, clusterController.getPodLister(), clusterController.getCrdLister(), clusterController.getCrdClient());
  }

  @AfterEach
  public void cleanUp() {
    client.close();
    server.after();
  }

  @Test
  public void testNonThreadedStateTransitions() {
    // initial state
    assertEquals(ClusterState.READY, clusterController.getClusterStateMachine().getState());
    // load spark-cluster-example.yaml
    SparkCluster cluster =
      clusterController.getCrdClient()
        .load(Thread.currentThread().getContextClassLoader().getResourceAsStream(Util.CLUSTER_EXAMPLE_PATH))
        .create();
    cluster.getMetadata().setUid("123456789");
    cluster.getMetadata().setNamespace(client.getNamespace());

    cluster = clusterController.getCrdClient().create(cluster);

    clusterController.process(cluster);

    // assume state INITIAL after cluster first arrived
    assertEquals(ClusterState.INITIAL, getState(clusterController));
    // check if status set, image set
    cluster = clusterController.getCrdClient().list().getItems().get(0);
    assertNotNull(cluster.getStatus());
    assertNotNull(cluster.getStatus().getImage());
    // check if configmaps deleted
    assertEquals(0, client.configMaps().list().getItems().size());
    // run create master
    clusterController.process(cluster);

    // check if state is create master
    assertEquals(ClusterState.CREATE_MASTER, getState(clusterController));
    // master pods created?
    List<Pod> createdMasterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
    assertEquals(cluster.getSpec().getMaster().getInstances(), createdMasterPods.size());

    // state should be create master until hostname arrives
    clusterController.process(cluster);
    assertEquals(ClusterState.CREATE_MASTER, getState(clusterController));

    // set node name
    Util.setNodeName(client, createdMasterPods, cluster);

    // when nodenames set -> wait for master hostname
    clusterController.process(cluster);
    assertEquals(ClusterState.WAIT_MASTER_HOST_NAME, getState(clusterController));
    // check if config map created
    List<ConfigMap> masterConfigMaps = controller.getConfigMaps(createdMasterPods, cluster);
    assertEquals(cluster.getSpec().getMaster().getInstances(), masterConfigMaps.size());

    // state should be wait master running
    clusterController.process(cluster);
    assertEquals(ClusterState.WAIT_MASTER_RUNNING, getState(clusterController));

    // set pods for running ...
    Util.setStatusRunning(client, createdMasterPods);
    // master pods running -> state should be create worker
    clusterController.process(cluster);
    assertEquals(ClusterState.CREATE_WORKER, getState(clusterController));
    // worker nodes created?
    List<Pod> createdWorkerPods = controller.getPodsByNode(cluster, cluster.getSpec().getWorker());
    assertEquals(cluster.getSpec().getWorker().getInstances(), createdWorkerPods.size());

    clusterController.process(cluster);
    assertEquals(ClusterState.CREATE_WORKER, getState(clusterController));
    // set worker nodenames
    Util.setNodeName(client, createdWorkerPods, cluster);

    // wait for worker host name
    clusterController.process(cluster);
    assertEquals(ClusterState.WAIT_WORKER_HOST_NAME, getState(clusterController));

    // wait for workers running
    clusterController.process(cluster);
    assertEquals(ClusterState.WAIT_WORKER_RUNNING, getState(clusterController));
    // set workers to running
    Util.setStatusRunning(client, createdWorkerPods);

    // workers running -> READY
    clusterController.process(cluster);
    assertEquals(ClusterState.READY, getState(clusterController));

    // delete worker and check for reconcile
    controller.deleteAllPods(cluster, cluster.getSpec().getWorker());
    clusterController.process(cluster);
    assertEquals(ClusterState.CREATE_WORKER, getState(clusterController));
  }

  private static ClusterState getState(SparkClusterController controller) {
    return controller.getClusterStateMachine().getState();
  }
}
