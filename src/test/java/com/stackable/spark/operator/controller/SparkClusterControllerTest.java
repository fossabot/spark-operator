package com.stackable.spark.operator.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.HttpURLConnection;

import org.junit.Rule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.SparkClusterList;
import com.stackable.spark.operator.cluster.crd.SparkNodeMaster;
import com.stackable.spark.operator.cluster.crd.SparkClusterSpec;
import com.stackable.spark.operator.cluster.crd.SparkNodeWorker;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import okhttp3.mockwebserver.RecordedRequest;

@EnableRuleMigrationSupport
public class SparkClusterControllerTest {
    @Rule
    public KubernetesServer server = new KubernetesServer();

    private static final CustomResourceDefinitionContext sparkClusterCRDContext = 
    		new CustomResourceDefinitionContext.Builder()
            	.withVersion("v1")
            	.withScope("Namespaced")
	            .withGroup("spark.stackable.de")
	            .withPlural("sparkclusters")
	            .build();
    
    private static final long RESYNC_PERIOD_MILLIS = 5 * 1000L;

    @Test
    @DisplayName("Should create master and worker pods with respect to a specified SparkCluster")
    public void testReconcile() throws InterruptedException {
        // Given
        String testNamespace = "defaultTest";
        SparkCluster testSparkCluster = getSparkCluster("spark-cluster", testNamespace, "abcdefg");
        server.expect().post()
        		.withPath("/api/v1/namespaces/" + testNamespace + "/pods")
                .andReturn(HttpURLConnection.HTTP_CREATED, new PodBuilder().withNewMetadata()
                												.withName("pod-clone")
                												.endMetadata()
                												.build())
                .times(testSparkCluster.getSpec().getMaster().getInstances() + 
                	   testSparkCluster.getSpec().getWorker().getInstances());
        
        KubernetesClient client = server.getClient();

        SharedInformerFactory informerFactory = client.informers();
        SharedIndexInformer<Pod> podSharedIndexInformer = 
        	informerFactory.sharedIndexInformerFor(Pod.class, PodList.class, RESYNC_PERIOD_MILLIS);
        
        SharedIndexInformer<SparkCluster> sparkClusterSharedIndexInformer = 
        	informerFactory.sharedIndexInformerForCustomResource(sparkClusterCRDContext, 
        														 SparkCluster.class, 
        														 SparkClusterList.class, 
        														 RESYNC_PERIOD_MILLIS);
        
        informerFactory.startAllRegisteredInformers();
        
        SparkClusterController sparkClusterController = 
        	new SparkClusterController(client, podSharedIndexInformer, sparkClusterSharedIndexInformer, testNamespace);

        sparkClusterController.reconcile(testSparkCluster, testSparkCluster.getSpec().getMaster());
        sparkClusterController.reconcile(testSparkCluster, testSparkCluster.getSpec().getWorker());

        RecordedRequest recordedRequest = server.getLastRequest();
        assertEquals("POST", recordedRequest.getMethod());
        assertTrue(recordedRequest.getBody().readUtf8().contains(testSparkCluster.getMetadata().getName()));
        
        informerFactory.stopAllRegisteredInformers();
    }

    private SparkCluster getSparkCluster(String name, String testNamespace, String uid) {
        SparkCluster sparkCluster = new SparkCluster();
        SparkClusterSpec sparkClusterSpec = new SparkClusterSpec();
        SparkNodeMaster master = new SparkNodeMaster();
        master.setInstances(2);
        sparkClusterSpec.setMaster(master);
        
        SparkNodeWorker worker = new SparkNodeWorker();
        worker.setInstances(3);
        sparkClusterSpec.setWorker(worker);

        sparkCluster.setSpec(sparkClusterSpec);
        sparkCluster.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(testNamespace).withUid(uid).build());
        return sparkCluster;
    }
}
