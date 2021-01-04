package tech.stackable.spark.operator.cluster.statemachine;

import common.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.SparkClusterController;
import tech.stackable.spark.operator.cluster.statemachine.SparkManagerStateMachine.ManagerState;
import tech.stackable.spark.operator.cluster.manager.crd.SparkManager;
import tech.stackable.spark.operator.cluster.manager.SparkManagerController;


import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestInstance(Lifecycle.PER_CLASS)
public class SparkManagerStateMachineTest {

  private KubernetesServer server;
  private KubernetesClient client;

  private SparkClusterController clusterController;
  private SparkManagerController managerController;

  @BeforeAll
  public void init() {
    server = new KubernetesServer(true, true);
    server.before();

    client = server.getClient();

    clusterController = new SparkClusterController(client, Util.CLUSTER_CRD_PATH, Util.RESYNC_CYCLE);
    clusterController.init();

    managerController = new SparkManagerController(client, Util.SYSTEMD_CRD_PATH, Util.CLUSTER_CRD_PATH, Util.RESYNC_CYCLE);
    managerController.init();
  }

  @AfterAll
  public void cleanUp() {
    client.close();
    server.after();
  }

  @Test
  public void testSparkSystemdStateMachineRestartTransition() {
    // load cluster example
    SparkCluster cluster = Util.loadSparkClusterExample(client, clusterController, Util.CLUSTER_EXAMPLE_PATH);
    assertNotNull(cluster);
    // load manager example
    SparkManager manager = Util.loadSparkManagerExample(client, managerController, Util.SYSTEMD_EXAMPLE_PATH);
    assertNotNull(manager);

    SparkManagerStateMachine sm = new SparkManagerStateMachine(clusterController);
    // start state
    assertEquals(sm.getState(), ManagerState.MANAGER_READY);

    // activate manager state machine in cluster
    managerController.process(manager);
    assertEquals(ManagerState.MANAGER_READY, sm.getState());

    clusterController.process(cluster);

  }
}
