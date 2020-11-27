package com.stackable.spark.operator;

import java.io.FileNotFoundException;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.common.SparkResourceCreator;
import com.stackable.spark.operator.controller.SparkClusterController;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;

/**
 * Main Class for Spark Operator: run via this command:
 *
 * mvn exec:java -Dexec.mainClass=com.stackable.spark.operator.SparkOperatorMain
 */
public class SparkOperatorMain {
	final static Logger logger = Logger.getLogger(SparkOperatorMain.class);

	// 10 seconds
    public static long RESYNC_CYCLE = 30 * 1000L;

    public static void main(String args[]) throws FileNotFoundException {
        try (KubernetesClient client = new DefaultKubernetesClient()) {
            String namespace = client.getNamespace();
            if (namespace == null) {
                logger.info("No namespace found via config, assuming default.");
                namespace = "default";
            }

            logger.info("Using namespace: " + namespace);
            
            // initialize CRDs for each controller
            String sparkClusterCRD = "spark-cluster-crd.yaml";
            SparkResourceCreator.createOrReplaceCRD(client, namespace, sparkClusterCRD);
            logger.info("/" + namespace + "/" + sparkClusterCRD +" created or replaced");
            String sparkApplicationCRD = "spark-application-crd.yaml";
            SparkResourceCreator.createOrReplaceCRD(client, namespace, sparkApplicationCRD);
            logger.info("/" + namespace + "/" + sparkApplicationCRD +" created or replaced");
            
            SharedInformerFactory informerFactory = client.informers();

            // TODO: remove hardcoded
            CustomResourceDefinitionContext crdContext =
            		new CustomResourceDefinitionContext.Builder()
                    .withVersion("v1")
                    .withScope("Namespaced")
                    .withGroup("spark.stackable.de")
                    .withPlural("sparkclusters")
                    .build();
            
            SparkClusterController sparkClusterController =
            	new SparkClusterController(client, informerFactory, crdContext, namespace, RESYNC_CYCLE);

            sparkClusterController.registerOtherEventHandler();
            
            informerFactory.startAllRegisteredInformers();
            informerFactory.addSharedInformerEventListener(
            	exception -> logger.fatal("Exception occurred, but caught. Missing CRD?\n" + exception));

            sparkClusterController.start();
        } catch (KubernetesClientException exception) {
            logger.fatal("Kubernetes Client Exception: " + exception.getMessage());
        }
    }

}
