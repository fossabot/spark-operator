package com.stackable.spark.operator.cluster.statemachine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.SparkClusterController;
import com.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine.ClusterState;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;

@TestInstance(Lifecycle.PER_CLASS)
public class SparkClusterStateMachineTest {
	public KubernetesServer server;
	public KubernetesClient client;
	
	public SparkClusterController controller;
	public Thread sparkClusterControllerThread;
	
	public String crdPath = "cluster/spark-cluster-crd.yaml";
	public String crdExamplePath = "cluster/spark-cluster-example.yaml";
	
	public long resyncCycle = 500 * 1000L;
	
	@BeforeAll
    public void init() {
		server = new KubernetesServer(true, true);
		server.before();
		
    	client = server.getClient();
    	controller = new SparkClusterController(client, crdPath, resyncCycle);
    	
		sparkClusterControllerThread = new Thread(controller);
		sparkClusterControllerThread.start();
    }

    @AfterAll
    public void cleanUp() {
    	client.close();
		server.after();
    }
    
    @Test
    public void testStateTransitions() throws InterruptedException {
    	assertEquals(ClusterState.READY, controller.getClusterStateMachine().getState());
    	// load spark-cluster-example.yaml
    	SparkCluster cluster = 
    			controller.getCrdClient()
        		.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(crdExamplePath))
        		.create();
    	cluster.getMetadata().setUid("123456789");
    	cluster.getMetadata().setNamespace(client.getNamespace());
    	
    	controller.getCrdClient().create(cluster);
    	
    	Thread.sleep(5000);
    	//assertEquals(ClusterState.INITIAL, controller.getClusterStateMachine().getState());
    	
    	sparkClusterControllerThread.interrupt();
    }
}
