package com.stackable.spark.operator.controller;

import java.util.List;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.SparkClusterDoneable;
import com.stackable.spark.operator.cluster.SparkClusterList;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatus;
import com.stackable.spark.operator.systemd.SparkSystemd;
import com.stackable.spark.operator.systemd.SparkSystemdDoneable;
import com.stackable.spark.operator.systemd.SparkSystemdList;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
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
	
	@Override
	protected void process(SparkSystemd systemd) {
		logger.trace("Got CRD: " + systemd.getMetadata().getName());
		// get cluster crd meta data
		List<HasMetadata> clusterMetaData = loadYaml(controllerCrdPath);
		CustomResourceDefinitionContext context = getCrdContext(clusterMetaData);
		
		// get custom crd client
		MixedOperation<SparkCluster,SparkClusterList,SparkClusterDoneable,Resource<SparkCluster, SparkClusterDoneable>> 
			clusterCrdClient = client.customResources(
									context, 
									SparkCluster.class, 
									SparkClusterList.class, 
									SparkClusterDoneable.class
								); 
		// get specific cluster via name
		SparkCluster cluster = 
			clusterCrdClient.inNamespace(namespace).withName(systemd.getSpec().getSparkClusterReference()).get();
		
		// set staged command in status
		cluster.setStatus(new SparkClusterStatus.Builder()
							.withSingleStagedCommand(systemd.getSpec().getSystemdAction())
							.build()
		);
		
		// update status
		clusterCrdClient.updateStatus(cluster);
		
		// remove systemd crd?
		if(crdClient.inNamespace(namespace).withName(systemd.getMetadata().getName()).delete()) {
			logger.trace("deleted systemd crd: " + systemd.getMetadata().getName() + " with action: " + systemd.getSpec().getSystemdAction());
		}
	}

}
