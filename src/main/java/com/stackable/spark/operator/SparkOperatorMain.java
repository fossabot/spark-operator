package com.stackable.spark.operator;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.SparkClusterList;
import com.stackable.spark.operator.controller.SparkClusterController;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;

/**
 * Main Class for Spark Operator: run via this command:
 *
 * mvn exec:java -Dexec.mainClass=com.stackable.spark.operator.SparkOperatorMain
 */
public class SparkOperatorMain {
	final static Logger logger = Logger.getLogger(SparkOperatorMain.class);

	// 10 seconds
    public static long RESYNC_CYCLE = 10 * 1000L;

    public static void main(String args[]) throws FileNotFoundException {
        try (KubernetesClient client = new DefaultKubernetesClient()) {
            String namespace = client.getNamespace();
            if (namespace == null) {
                logger.info("No namespace found via config, assuming default.");
                namespace = "default";
            }

            logger.info("Using namespace: " + namespace);
            
            // initialize CRDs
            createOrReplaceCRD(client, namespace, "spark-cluster-crd.yaml");
            
            CustomResourceDefinitionContext sparkClusterCRDContext =
            		new CustomResourceDefinitionContext.Builder()
                    .withVersion("v1")
                    .withScope("Namespaced")
                    .withGroup("spark.stackable.de")
                    .withPlural("sparkclusters")
                    .build();

            SharedInformerFactory informerFactory = client.informers();

            SharedIndexInformer<Pod> podSharedIndexInformer =
            	informerFactory.sharedIndexInformerFor(Pod.class, PodList.class, RESYNC_CYCLE);

            SharedIndexInformer<SparkCluster> sparkClusterSharedIndexInformer =
            	informerFactory.sharedIndexInformerForCustomResource(sparkClusterCRDContext,
            														 SparkCluster.class,
            														 SparkClusterList.class,
            														 RESYNC_CYCLE);

            SparkClusterController sparkClusterController =
            		new SparkClusterController(client, podSharedIndexInformer, sparkClusterSharedIndexInformer, namespace);

            sparkClusterController.create();
            informerFactory.startAllRegisteredInformers();
            informerFactory.addSharedInformerEventListener(
            		exception -> logger.fatal("Exception occurred, but caught. Missing CRD?\n" + exception));

            sparkClusterController.run();
        } catch (KubernetesClientException exception) {
            logger.fatal("Kubernetes Client Exception: " + exception.getMessage());
        }
    }
    
    private static List<HasMetadata> createOrReplaceCRD(KubernetesClient client, String namespace, String path) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(path);
        
    	// Load sparkcluster crd into kubernetes 
    	List<HasMetadata> result = client.load(is).get();
    	client.resourceList(result).inNamespace(namespace).createOrReplace(); 
    	logger.info("/" + namespace + "/" + path +" created or replaced");
    	return result;
    }
}
