package com.stackable.spark.operator.systemd;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import com.stackable.spark.operator.abstractcontroller.crd.CrdClassDoneable;
import com.stackable.spark.operator.abstractcontroller.crd.CrdClassList;
import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatus;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatusSystemd;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * The spark systemd controller is responsible for cluster restarts.
 * Signal the spark cluster controller of pending restarts via editing the respective SparkCluster
 */
public class SparkSystemdController extends AbstractCrdController<SparkSystemd> {
	private static final Logger logger = Logger.getLogger(SparkSystemdController.class.getName());
	
	private String controllerCrdPath;
	
    public SparkSystemdController(KubernetesClient client, String crdPath, String controllerCrdPath, Long resyncCycle) {
		super(client, crdPath, resyncCycle);
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
		// get custom crd client
		MixedOperation<
		SparkCluster,
		CrdClassList<SparkCluster>,
		CrdClassDoneable<SparkCluster>,
		Resource<SparkCluster, CrdClassDoneable<SparkCluster>>> 
			clusterCrdClient = getCustomCrdClient(controllerCrdPath, new SparkCluster());
		
		SparkCluster cluster = clusterCrdClient.inNamespace(namespace).withName(systemd.getSpec().getSparkClusterReference()).get();
		
		// set staged command in status
		SparkClusterStatus status = cluster.getStatus();
		if(status == null) {
			status = new SparkClusterStatus();
		}
		
		status.setSystemd(new SparkClusterStatusSystemd.Builder()
							.withSingleStagedCommand(systemd.getSpec().getSystemdAction())
							.build()
						 );
		
		cluster.setStatus(status);
		
		// update status
		clusterCrdClient.updateStatus(cluster);
		
		// remove systemd crd?
		if(crdClient.inNamespace(namespace).withName(systemd.getMetadata().getName()).delete()) {
			logger.trace("deleted systemd crd: " + systemd.getMetadata().getName() + " with action: " + systemd.getSpec().getSystemdAction());
		}
	}

}
