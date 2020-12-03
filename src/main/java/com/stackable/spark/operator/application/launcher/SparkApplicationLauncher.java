package com.stackable.spark.operator.application.launcher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;

import com.stackable.spark.operator.application.SparkApplication;
import com.stackable.spark.operator.application.crd.SparkApplicationSpec;
import com.stackable.spark.operator.common.type.SparkConfig;

public class SparkApplicationLauncher {
	public static final Logger logger = Logger.getLogger(SparkApplicationLauncher.class.getName());
	
	public void launch(SparkApplication app) {
		SparkApplicationSpec spec = app.getSpec();
		try {
			final CountDownLatch countDownLatch = new CountDownLatch(1);
			SparkApplicationListener sparkAppListener = new SparkApplicationListener(countDownLatch);
		
			new SparkLauncher()
				//.setSparkHome("/home/bawa/Downloads/krustlet/work/parcels/spark-3.0.1/spark-3.0.1-bin-hadoop2.7")
				//.setAppResource("/home/bawa/Downloads/krustlet/work/parcels/spark-3.0.1/spark-3.0.1-bin-hadoop2.7/examples/jars/spark-examples_2.12-3.0.1.jar")
				.setAppResource(spec.getMainClass())	
				.setMainClass(spec.getMainClass())
				// TODO get host
				.setMaster("spark://bawa-virtualbox:7077")
				.setDeployMode("cluster")
				.setAppName(app.getMetadata().getName())
				.setConf(SparkConfig.SPARK_DRIVER_CORES.getConfig(), spec.getDriver().getCores())
				.setConf(SparkConfig.SPARK_DRIVER_MEMORY.getConfig(), spec.getDriver().getMemory())
				.setConf(SparkConfig.SPARK_EXECUTOR_CORES.getConfig(), spec.getExecutor().getCores())
				.setConf(SparkConfig.SPARK_EXECUTOR_MEMORY.getConfig(), spec.getExecutor().getMemory())
				.addAppArgs((String[]) spec.getArgs().toArray())
				.startApplication(sparkAppListener);

			Thread sparkAppListenerThread = new Thread(sparkAppListener);
			sparkAppListenerThread.start();
			long timeout = 120;
			countDownLatch.await(timeout, TimeUnit.SECONDS);
		}
		catch(InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}
}
