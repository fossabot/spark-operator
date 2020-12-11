package com.stackable.spark.operator;

import java.io.FileNotFoundException;

import com.stackable.spark.operator.controller.SparkApplicationController;
import com.stackable.spark.operator.controller.SparkClusterController;
import com.stackable.spark.operator.controller.SparkSystemdController;

/**
 * Main Class for Spark Operator: run via this command:
 *
 * mvn exec:java -Dexec.mainClass=com.stackable.spark.operator.SparkOperatorMain
 */
public class SparkOperatorMain {
	// 120 seconds
    public static long RESYNC_CYCLE = 300 * 1000L;
    
    private static String CLUSTER_CRD_PATH = "cluster/spark-cluster-crd.yaml";
	private static String SYSTEMD_CRD_PATH = "systemd/spark-systemd-crd.yaml";
	private static String APPLICATION_CRD_PATH = "application/spark-application-crd.yaml";

    public static void main(String args[]) throws FileNotFoundException {
        SparkClusterController sparkClusterController = new SparkClusterController(
        	CLUSTER_CRD_PATH, 
			RESYNC_CYCLE
        );
        
		SparkSystemdController sparkSystemdController = new SparkSystemdController(
			SYSTEMD_CRD_PATH, 
			CLUSTER_CRD_PATH,
			RESYNC_CYCLE
		);
        
        SparkApplicationController sparkApplicationController = new SparkApplicationController(
			APPLICATION_CRD_PATH,
			RESYNC_CYCLE
    	);

		// start different controllers
		Thread sparkClusterControllerThread = new Thread(sparkClusterController);
		sparkClusterControllerThread.start();
 
		Thread sparkSystemdThread = new Thread(sparkSystemdController);
		sparkSystemdThread.start();
		
		Thread sparkApplicationThread = new Thread(sparkApplicationController);
		sparkApplicationThread.start();
    }

}
