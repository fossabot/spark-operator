package com.stackable.spark.operator.application;

import java.util.List;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import com.stackable.spark.operator.application.launcher.SparkApplicationLauncher;
import com.stackable.spark.operator.cluster.crd.SparkNodeMaster;
import com.stackable.spark.operator.common.type.SparkConfig;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;

public class SparkApplicationController extends	AbstractCrdController<SparkApplication> {

	private static final Logger logger = Logger.getLogger(SparkApplicationController.class.getName());
	
	private SparkApplicationLauncher sparkApplicationLauncher;
	
	public SparkApplicationController(KubernetesClient client, String crdPath, Long resyncCycle) {
		super(client, crdPath, resyncCycle);
		
		sparkApplicationLauncher = new SparkApplicationLauncher();
	}

	@Override
	protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced());
		logger.info("SparkApplication informer initialized ... waiting for changes");
	}
    
	@Override
	protected void process(SparkApplication app) {
		logger.trace("Got CRD: " + app.getMetadata().getName());
		sparkApplicationLauncher.launch(app, this);
	}
	
	/**
	 * Return master node name and port for given cluster
	 * @param clusterName - name of cluster definiton
	 * @return spark master host name
	 */
	public String getMasterNodeName(String clusterName) {
		PodList podList = client.pods().inNamespace(namespace).list();
		
		if(podList == null ) {
			logger.warn("no pods found in namespace " + namespace);
			return null;
		}
			
		List<Pod> pods = podList.getItems();
		for(Pod pod: pods) {
			if(pod.getMetadata().getOwnerReferences().size() == 0 || 
				!pod.getMetadata().getOwnerReferences().get(0).getKind().equals("SparkCluster") ) { 
				continue;
			}
			
			// has nodename?
			String nodeName = pod.getSpec().getNodeName();
			if( nodeName == null ) {
				continue;
			}
			
			// is master?
			if(!pod.getMetadata().getName().contains(SparkNodeMaster.POD_TYPE)) {
				continue;
			}
			
			// has container?
			if(pod.getSpec().getContainers().size() == 0) {
				continue;
			}
			
			String port = "7077";
			
			// has SPARK_MASTER_PORT?
			for(Container container: pod.getSpec().getContainers()) {
				if(container.getEnv().size() == 0) continue;
				
				for(EnvVar var: container.getEnv()) {
					if(var.getName().equals(SparkConfig.SPARK_MASTER_PORT.getEnv()))
						port = var.getValue();
				}
			}
			
			//construct
			return "spark://" + nodeName + ":" + port;
		}
		
		return null;
	}

}
