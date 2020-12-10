package com.stackable.spark.operator.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.systemd.SparkSystemd;
import com.stackable.spark.operator.systemd.SparkSystemdDoneable;
import com.stackable.spark.operator.systemd.SparkSystemdList;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

/**
 * The spark systemd controller is responsible for cluster restarts.
 * Signal the spark cluster controller of pending restarts via editing the respective SparkCluster
 */
public class SparkSystemdController extends AbstractCrdController<SparkSystemd, SparkSystemdList, SparkSystemdDoneable> {
	private static final Logger logger = Logger.getLogger(SparkSystemdController.class.getName());
	
	private String controllerCrdPath;
	
    public SparkSystemdController(String crdPath, String controllerCrdPath, Long resyncCycle) {
		super(crdPath, resyncCycle);
		this.controllerCrdPath = controllerCrdPath;
	}

	@Override
	protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced());
		logger.info("SparkSystemd informer initialized ... waiting for changes");
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected void process(SparkSystemd systemd) {
		logger.trace("Got CRD: " + systemd.getMetadata().getName());
		// get cluster crd meta data
		List<HasMetadata> clusterMetaData = loadYaml(controllerCrdPath);
		CustomResourceDefinition crd = (CustomResourceDefinition) clusterMetaData.get(0);
		CustomResourceDefinitionContext context = getCrdContext(clusterMetaData);
		
		Map<String, Object> map = 
			client.customResource(context).get(
				crd.getMetadata().getNamespace(),
				systemd.getSpec().getSparkClusterReference()
			);
		
		// signal systemd command to SparkClusterController
		// change something
		Object x = map.get("spec");
		((Map<String,Object>)x).put("systemd", systemd.getSpec().getSystemdAction());
		
		try {
			client.customResource(context).edit(
				crd.getMetadata().getNamespace(), 
				systemd.getSpec().getSparkClusterReference(), 
				map
			);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// remove crd?
		if(crdClient.inNamespace(namespace).withName(systemd.getMetadata().getName()).delete()) {
			logger.debug("deleted: " + systemd.getMetadata().getName());
		}
	}

}
