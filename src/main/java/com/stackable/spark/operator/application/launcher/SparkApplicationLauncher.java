package com.stackable.spark.operator.application.launcher;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.spark.launcher.SparkLauncher;

import com.stackable.spark.operator.application.SparkApplication;
import com.stackable.spark.operator.application.crd.SparkApplicationSpec;
import com.stackable.spark.operator.common.type.SparkConfig;
import com.stackable.spark.operator.controller.SparkApplicationController;

import io.fabric8.kubernetes.api.model.EnvVar;

/**
 * SparkApplicationLauncher starts a spark job via spark-submit.sh
 */
public class SparkApplicationLauncher {
	private Set<SparkApplication> workingQueue = new HashSet<SparkApplication>();
	
	/**
	 * Launch the given application, transform config parameters, set executor and driver options etc.
	 * @param app - spark application configured via yaml/json
	 * @param controller - spark application controller to remove finished / failed applications
	 */
	public void launch(SparkApplication app, SparkApplicationController controller) {
		// dont act on processed apps
		if(workingQueue.contains(app)) {
			return;
		}
		workingQueue.add(app);
		
		SparkApplicationSpec spec = app.getSpec();
		try {
			final CountDownLatch countDownLatch = new CountDownLatch(1);
			SparkApplicationListener sparkAppListener = new SparkApplicationListener(countDownLatch);
			
			URL sparkHome = ClassLoader.getSystemResource("spark-3.0.1-bin-hadoop2.7");
		
			SparkLauncher launcher = new SparkLauncher();
				launcher.setSparkHome(sparkHome.getPath());
				launcher.setAppResource(spec.getMainApplicationFile());	
				launcher.setMainClass(spec.getMainClass());
				// TODO get host
				launcher.setMaster(controller.getMasterNodeName(null));
				launcher.setDeployMode(spec.getMode());
				launcher.setAppName(app.getMetadata().getName());
				// conf from app
				launcher.setConf(SparkConfig.SPARK_DRIVER_CORES.getConfig(), spec.getDriver().getCores());
				launcher.setConf(SparkConfig.SPARK_DRIVER_MEMORY.getConfig(), spec.getDriver().getMemory());
				launcher.setConf(SparkConfig.SPARK_EXECUTOR_CORES.getConfig(), spec.getExecutor().getCores());
				launcher.setConf(SparkConfig.SPARK_EXECUTOR_MEMORY.getConfig(), spec.getExecutor().getMemory());
				// add other spark configuration
				for(EnvVar var: spec.getSparkConfiguration()) {
		    		String name = var.getName();
		    		String value= var.getValue();
		    		if(name != null && !name.isEmpty() && value != null && !value.isEmpty()) {
		    			launcher.setConf(name, value);	
		    		}
				}
				launcher.addAppArgs(spec.getArgs().toArray(new String[spec.getArgs().size()]));
				launcher.startApplication(sparkAppListener);

			Thread sparkAppListenerThread = new Thread(sparkAppListener);
			sparkAppListenerThread.start();
			
			long timeout = 120;
			countDownLatch.await(timeout, TimeUnit.SECONDS);
			
			// delete app when finished
			// TODO: check for sleep / timer if repeating
			workingQueue.remove(app);
			controller.getCrdClient().delete(app);
		}
		catch(InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}
}
