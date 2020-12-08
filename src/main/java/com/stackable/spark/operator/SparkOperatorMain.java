package com.stackable.spark.operator;

import java.io.FileNotFoundException;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.controller.SparkApplicationController;
import com.stackable.spark.operator.controller.SparkClusterController;
import com.stackable.spark.operator.controller.SparkSystemdController;

/**
 * Main Class for Spark Operator: run via this command:
 *
 * mvn exec:java -Dexec.mainClass=com.stackable.spark.operator.SparkOperatorMain
 */
public class SparkOperatorMain {
	final static Logger logger = Logger.getLogger(SparkOperatorMain.class);
	// 120 seconds
    public static long RESYNC_CYCLE = 120 * 1000L;

    public static void main(String args[]) throws FileNotFoundException {
        SparkClusterController sparkClusterController = new SparkClusterController(
			"cluster/spark-cluster-crd.yaml", 
			RESYNC_CYCLE
        );
        
		SparkSystemdController sparkSystemdController = new SparkSystemdController(
			"systemd/spark-systemd-crd.yaml", 
			RESYNC_CYCLE
		);
        
        SparkApplicationController sparkApplicationController = new SparkApplicationController(
			"application/spark-application-crd.yaml",
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
