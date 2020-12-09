package com.stackable.spark.operator.controller;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.systemd.SparkSystemd;
import com.stackable.spark.operator.systemd.SparkSystemdDoneable;
import com.stackable.spark.operator.systemd.SparkSystemdList;

public class SparkSystemdController extends AbstractCrdController<SparkSystemd, SparkSystemdList, SparkSystemdDoneable> {
	private static final Logger logger = Logger.getLogger(SparkSystemdController.class.getName());
	
    public SparkSystemdController(String crdPath, Long resyncCycle) {
		super(crdPath, resyncCycle);
	}

	@Override
	protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced());
		logger.info("SparkSystemd informer initialized ... waiting for changes");
	}
	
	@Override
	protected void process(SparkSystemd systemd) {
		logger.trace("Got CRD: " + systemd.getMetadata().getName());
		// signal systemd command to SparkClusterController
		
		// remove crd?
		//crdClient.inNamespace(namespace).withName(systemd.getMetadata().getName()).delete();
	}

}
