package tech.stackable.spark.operator.cluster.statemachine;

import common.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import tech.stackable.spark.operator.cluster.SparkCluster;
import tech.stackable.spark.operator.cluster.SparkClusterController;
import tech.stackable.spark.operator.cluster.statemachine.SparkSystemdStateMachine.SystemdState;
import tech.stackable.spark.operator.systemd.SparkSystemd;
import tech.stackable.spark.operator.systemd.SparkSystemdController;


import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestInstance(Lifecycle.PER_CLASS)
public class SparkSystemdStateMachineTest {

  private KubernetesServer server;
  private KubernetesClient client;

  private SparkClusterController clusterController;
  private SparkSystemdController systemdController;

  @BeforeAll
  public void init() {
    server = new KubernetesServer(true, true);
    server.before();

    client = server.getClient();

    clusterController = new SparkClusterController(client, Util.CLUSTER_CRD_PATH, Util.RESYNC_CYCLE);
    clusterController.init();

    systemdController = new SparkSystemdController(client, Util.SYSTEMD_CRD_PATH, Util.CLUSTER_CRD_PATH, Util.RESYNC_CYCLE);
    systemdController.init();
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
    // load systemd example
    SparkSystemd systemd = Util.loadSparkSystemdExample(client, systemdController, Util.SYSTEMD_EXAMPLE_PATH);
    assertNotNull(systemd);

    SparkSystemdStateMachine sm = new SparkSystemdStateMachine(clusterController);
    // start state
    assertEquals(sm.getState(), SystemdState.SYSTEMD_READY);

  }
}
