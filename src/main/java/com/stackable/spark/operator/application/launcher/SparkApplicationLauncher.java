package com.stackable.spark.operator.application.launcher;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
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
    private static final Logger logger = Logger.getLogger(SparkApplicationLauncher.class.getName());
    
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
			logger.debug("Start app: " + app.getMetadata().getName() + " - SPARK_HOME: " + sparkHome.getPath());
		
			SparkLauncher launcher = new SparkLauncher();
				launcher.setSparkHome(sparkHome.getPath());
				launcher.setAppResource(spec.getMainApplicationFile());	
				launcher.setMainClass(spec.getMainClass());
				// TODO get host
				launcher.setMaster(controller.getMasterNodeName(null));
				launcher.setDeployMode(spec.getMode());
				launcher.setAppName(app.getMetadata().getName());
				// conf from app
				if(spec.getDriver().getCores() != null)
					launcher.setConf(SparkConfig.SPARK_DRIVER_CORES.getConfig(), spec.getDriver().getCores());
				if(spec.getDriver().getMemory() != null)
					launcher.setConf(SparkConfig.SPARK_DRIVER_MEMORY.getConfig(), spec.getDriver().getMemory());
				if(spec.getExecutor().getCores() != null)
					launcher.setConf(SparkConfig.SPARK_EXECUTOR_CORES.getConfig(), spec.getExecutor().getCores());
				if(spec.getExecutor().getMemory() != null)
					launcher.setConf(SparkConfig.SPARK_EXECUTOR_MEMORY.getConfig(), spec.getExecutor().getMemory());
				if(spec.getSecret() != null)
					launcher.setConf(SparkConfig.SPARK_AUTHENTICATE.getConfig(), "true");
				if(spec.getSecret() != null)
					launcher.setConf(SparkConfig.SPARK_AUTHENTICATE_SECRET.getConfig(), spec.getSecret());
				// add other spark configuration
				for(EnvVar var: spec.getSparkConfiguration()) {
		    		String name = var.getName();
		    		String value= var.getValue();
		    		if(name != null && !name.isEmpty() && value != null && !value.isEmpty()) {
		    			launcher.setConf(name, value);	
		    		}
				}
				if(spec.getArgs().size() > 0)
					launcher.addAppArgs(spec.getArgs().toArray(new String[spec.getArgs().size()]));
				// start with listener
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
