package com.stackable.spark.operator.application.launcher;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;

public class SparkApplicationListener  implements SparkAppHandle.Listener, Runnable {
	private static final Logger logger = Logger.getLogger(SparkApplicationListener.class.getName());
	
    private final CountDownLatch countDownLatch;
    
    public SparkApplicationListener(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }
    
    @Override
    public void stateChanged(SparkAppHandle handle) {
        String sparkAppId = handle.getAppId();
        State appState = handle.getState();
        if (sparkAppId != null) {
            logger.info("Spark job with app id: " + sparkAppId + ",\t State changed to: " + appState);
        } else {
        	logger.info("Spark job's state changed to: " + appState);
        }
        if (appState != null && appState.isFinal()) {
            countDownLatch.countDown();
        }
    }
    @Override
    public void infoChanged(SparkAppHandle handle) {}
    @Override
    public void run() {}
}
