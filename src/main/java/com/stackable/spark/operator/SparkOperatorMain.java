package com.stackable.spark.operator;

import java.io.FileNotFoundException;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.controller.SparkApplicationController;
import com.stackable.spark.operator.controller.SparkClusterController;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;

/**
 * Main Class for Spark Operator: run via this command:
 *
 * mvn exec:java -Dexec.mainClass=com.stackable.spark.operator.SparkOperatorMain
 */
public class SparkOperatorMain {
	final static Logger logger = Logger.getLogger(SparkOperatorMain.class);

	// 10 seconds
    public static long RESYNC_CYCLE = 5 * 1000L;

    public static void main(String args[]) throws FileNotFoundException {
        try (KubernetesClient client = new DefaultKubernetesClient()) {
            String namespace = client.getNamespace();
            
            if (namespace == null) {
            	namespace = "default";
                logger.info("No namespace found via config, assuming " + namespace);
            }

            logger.info("Using namespace: " + namespace);
            
            SharedInformerFactory informerFactory = client.informers();
            
            SparkApplicationController sparkApplicationController =	new SparkApplicationController(
    			client, 
    			informerFactory, 
    			namespace, 
    			"application/spark-application-crd.yaml",
    			RESYNC_CYCLE
        	);
            
            SparkClusterController sparkClusterController = new SparkClusterController(
    			client, 
    			informerFactory, 
    			namespace, 
    			"cluster/spark-cluster-crd.yaml", 
    			RESYNC_CYCLE
            );

            informerFactory.addSharedInformerEventListener(
                	exception -> logger.fatal("Tip: missing CRDs?\n" + exception));

            informerFactory.startAllRegisteredInformers();
 
            // start different controllers
            Thread sparkClusterThread = new Thread(sparkClusterController);
            sparkClusterThread.start();
            
			// sleep for initialization
            Thread.sleep(2000);
            
            Thread sparkApplicationThread = new Thread(sparkApplicationController);
            sparkApplicationThread.start();


        } catch (KubernetesClientException exception) {
            logger.fatal("Kubernetes Client Exception: " + exception.getMessage());
        } catch (InterruptedException e) {
			e.printStackTrace();
		}
    }

}
