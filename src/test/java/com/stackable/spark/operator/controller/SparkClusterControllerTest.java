package com.stackable.spark.operator.controller;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.stackable.spark.operator.cluster.SparkClusterController;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;

@TestInstance(Lifecycle.PER_CLASS)
public class SparkClusterControllerTest {
	public KubernetesServer server;
	
	public KubernetesClient client;
	public SharedInformerFactory informerFactory;
	
	public SparkClusterController controller;
	
	public String namespace = "default";
	public String crdPath = "cluster/spark-cluster-crd.yaml";
	
	public long resyncCycle = 5 * 1000L;
	
	@BeforeAll
    public void init() {
		server = new KubernetesServer(true, true);
		server.before();
		
    	client = server.getClient();
    	informerFactory = client.informers();
    	controller = new SparkClusterController(crdPath, resyncCycle);
    	informerFactory.startAllRegisteredInformers();
    }

    @AfterAll
    public void cleanUp() {
    	informerFactory.stopAllRegisteredInformers();
    	client.close();
		server.after();
    }
    
    @Test
    @DisplayName("Should list all SparkCluster custom resources")
    public void testSparkClusterList() {
        // Given
        //KubernetesClient client = server.getClient();

    }

}
