package com.stackable.spark.operator.cluster.statemachine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.stackable.spark.operator.cluster.SparkClusterController;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;

@TestInstance(Lifecycle.PER_CLASS)
public class SparkSystemdStateMachineTest {
	public KubernetesServer server;
	public KubernetesClient client;
	
	public SparkClusterController controller;
	
	public String namespace = "default";
	public String crdPath = "cluster/spark-cluster-crd.yaml";
	
	public long resyncCycle = 5 * 1000L;
	
	@BeforeAll
    public void init() {
		server = new KubernetesServer(true, true);
		server.before();
		
    	client = server.getClient();
    	controller = new SparkClusterController(client, crdPath, resyncCycle);
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
