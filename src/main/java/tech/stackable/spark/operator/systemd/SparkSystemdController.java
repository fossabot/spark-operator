package tech.stackable.spark.operator.systemd;

import java.util.List;

import org.apache.log4j.Logger;

import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import tech.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import tech.stackable.spark.operator.cluster.SparkCluster;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatus;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusSystemd;
import tech.stackable.spark.operator.common.fabric8.SparkSystemdDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkSystemdList;

/**
 * The spark systemd controller is responsible for cluster restarts.
 * Signal the spark cluster controller of pending restarts via editing the respective SparkCluster
 */
public class SparkSystemdController extends AbstractCrdController<SparkSystemd,SparkSystemdList,SparkSystemdDoneable> {
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
	public void process(SparkSystemd systemd) {
		logger.trace("Got CRD: " + systemd.getMetadata().getName());
		// get custom crd client
		MixedOperation<SparkCluster, CustomResourceList<SparkCluster>, CustomResourceDoneable<SparkCluster>, Resource<SparkCluster, CustomResourceDoneable<SparkCluster>>> 
			clusterCrdClient = getCustomCrdClient(controllerCrdPath, SparkCluster.class);
		
		SparkCluster cluster = clusterCrdClient.inNamespace(namespace).withName(systemd.getSpec().getSparkClusterReference()).get();
		
		// status available?
		SparkClusterStatus status = cluster == null ? null : cluster.getStatus();
		
		if(status == null) {
			status = new SparkClusterStatus();
		}
		// no staged commands available
		if(status.getSystemd() == null) {
			status.setSystemd(new SparkClusterStatusSystemd.Builder()
				.withSingleStagedCommand(systemd.getSpec().getSystemdAction())
				.build()
			);
		}
		// already existing staged commands
		else {
			List<String> stagedCommands = status.getSystemd().getStagedCommands();
			stagedCommands.add(systemd.getSpec().getSystemdAction());
		}
		
		cluster.setStatus(status);
		
		try {
			// update status
			clusterCrdClient.updateStatus(cluster);
		}
		catch(KubernetesClientException ex) {
			logger.warn("Received outdated object: " + ex.getMessage());
		}
		// remove systemd crd?
		if(crdClient.inNamespace(namespace).withName(systemd.getMetadata().getName()).delete()) {
			logger.trace("deleted systemd crd: " + systemd.getMetadata().getName() + " with action: " + systemd.getSpec().getSystemdAction());
		}
	}

}
