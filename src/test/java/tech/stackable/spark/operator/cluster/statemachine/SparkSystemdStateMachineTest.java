package tech.stackable.spark.operator.cluster.statemachine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import tech.stackable.spark.operator.cluster.SparkClusterController;

@TestInstance(Lifecycle.PER_CLASS)
public class SparkSystemdStateMachineTest {

  private KubernetesServer server;
  private KubernetesClient client;

  private SparkClusterController controller;

  private static final String CRD_PATH = "cluster/spark-cluster-crd.yaml";

  private static final long RESYNC_CYCLE = 5 * 1000L;

  @BeforeAll
  public void init() {
    server = new KubernetesServer(true, true);
    server.before();

    client = server.getClient();
    controller = new SparkClusterController(client, CRD_PATH, RESYNC_CYCLE);
  }

  @AfterAll
  public void cleanUp() {
    client.close();
    server.after();
  }

//    @Test
//    @DisplayName("Check systemd statemachine restart event transitions")
//    public void testSparkSystemdStateMachineRestartTransition() {
//    	SparkSystemdStateMachine sm = new SparkSystemdStateMachine(controller);
//    	SparkCluster cluster = new SparkCluster();
//    	// start state
//    	assertEquals(sm.getState(), SystemdState.SYSTEMD_READY);
//    	
//    	sm.transition(cluster, SystemdEvent.RESTART);
//    	assertEquals(sm.getState(), SystemdState.SYSTEMD_JOBS_FINISHED);
//
//    	sm.transition(cluster, SystemdEvent.JOBS_FINISHED);
//    	assertEquals(sm.getState(), SystemdState.SYSTEMD_PODS_DELETED);
//    }
}
