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
            logger.info("spark job[" + sparkAppId + "] state changed to: " + appState);
        } else {
        	logger.info("spark job state changed to: " + appState);
        }
        if (appState != null && appState.isFinal()) {
        	logger.info("spark job[" + sparkAppId + "] state changed to: " + appState);
            countDownLatch.countDown();
        }
    }
    @Override
    public void infoChanged(SparkAppHandle handle) {
        String sparkAppId = handle.getAppId();
        State appState = handle.getState();
        if (sparkAppId != null) {
            logger.info("spark job info[" + sparkAppId + "] state changed to: " + appState);
        } else {
        	logger.info("spark job info changed to: " + appState);
        }
    }
    @Override
    public void run() {}
}
