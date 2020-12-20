package com.stackable.spark.operator.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import com.stackable.spark.operator.cluster.crd.SparkNode;
import com.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine;
import com.stackable.spark.operator.cluster.statemachine.SparkSystemdStateMachine;
import com.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import com.stackable.spark.operator.common.fabric8.SparkClusterList;
import com.stackable.spark.operator.common.state.PodState;
import com.stackable.spark.operator.common.type.SparkConfig;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;

/**
 * The SparkClusterController is responsible for installing the spark master and workers.
 * Scale up and down to the instances required in the specification.
 * Offer systemd functionality for start, stop and restart the cluster
 */
public class SparkClusterController extends AbstractCrdController<SparkCluster,SparkClusterList,SparkClusterDoneable> {
    private static final Logger logger = Logger.getLogger(SparkClusterController.class.getName());

    private SharedIndexInformer<Pod> podInformer;
    private Lister<Pod> podLister;
    
    private SparkSystemdStateMachine systemdStateMachine;
    private SparkClusterStateMachine clusterStateMachine;
    
	public SparkClusterController(KubernetesClient client, String crdPath, Long resyncCycle) {
		super(client, crdPath, resyncCycle);
		
        this.podInformer = informerFactory.sharedIndexInformerFor(Pod.class, PodList.class, resyncCycle);
        this.podLister = new Lister<>(podInformer.getIndexer(), namespace);

        this.systemdStateMachine = new SparkSystemdStateMachine(this);
        this.clusterStateMachine = new SparkClusterStateMachine(this);
        // register pods
        registerPodEventHandler();
    }
	
	@Override
	protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced() || !podInformer.hasSynced());
		logger.info("SparkCluster informer and pod informer initialized ... waiting for changes");
	}
	
	/**
	 * Register event handler for kubernetes pods
	 */
	private void registerPodEventHandler() {
		podInformer.addEventHandler(new ResourceEventHandler<Pod>() {
            @Override
            public void onAdd(Pod pod) {
            	logger.trace("onAddPod: " + pod);
                handlePodObject(pod);
            }
            @Override
            public void onUpdate(Pod oldPod, Pod newPod) {
                if (oldPod.getMetadata().getResourceVersion().equals(newPod.getMetadata().getResourceVersion())) {
                    return;
                }
                logger.trace("onUpdate:\npodOld: " + oldPod + "\npodNew: " + newPod);
                handlePodObject(newPod);
            }
            @Override
            public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
            	logger.trace("onDeletePod: " + pod);
            }
        });
	}

	@Override
	protected void process(SparkCluster cluster) {
		// TODO: except stopped?
		// only go for cluster state machine if no systemd action is currently running
		if(systemdStateMachine.process(cluster) == true) {
			return;
		}
		clusterStateMachine.process(cluster);
	}
	
	/**
	 * Create pods with regard to spec and current state
	 * @param pods - list of available pods belonging to the given node
	 * @param cluster - current spark cluster
	 * @param node - master or worker node
	 * @return list of created pods
	 */
    public List<Pod> createPods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    	List<Pod> createdPods = new ArrayList<Pod>();
        // Compare with desired state (spec.master.node.instances)
        // If less then create new pods
        if (pods.size() < node.getInstances()) {
            for (int index = 0; index < node.getInstances() - pods.size(); index++) {
                Pod pod = client.pods().inNamespace(cluster.getMetadata().getNamespace()).create(createNewPod(cluster, node));
                createdPods.add(pod);
            }
        }
        return createdPods;
    }
    
	/**
	 * Delete pods with regard to spec and current state -> for reconcilation
	 * @param pods - list of available pods belonging to the given node
	 * @param cluster - current spark cluster
	 * @param node - master or worker node
	 * @return list of deleted pods
	 */
    public List<Pod> deletePods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    	List<Pod> deletedPods = new ArrayList<Pod>();
        // If more pods than spec delete old pods
        int diff = pods.size() - node.getInstances();

        for (; diff > 0; diff--) {
        	// TODO: dont remove current master leader!
            Pod pod = pods.remove(0);
            client.pods()
            	.inNamespace(cluster.getMetadata().getNamespace())
            	.withName(pod.getMetadata().getName())
            	.delete();
            deletedPods.add(pod);
        }
		logger.debug(String.format("[%s] - deleted %d %s pod(s): %s", 
				clusterStateMachine.getState().name(), deletedPods.size(), node.getPodTypeName(), podListToDebug(deletedPods)));
        return deletedPods;
    }
    
    /**
     * Delete all node (master/worker) pods in cluster with no regard to spec -> systemd
     * @param cluster - cluster specification to retrieve all used pods
     * @return list of deleted pods
     */
    public List<Pod> deletePods(SparkCluster cluster, String state, SparkNode ...nodes) {
    	List<Pod> deletedPods = new ArrayList<Pod>();

        // if nodes are null take all
        if(nodes == null || nodes.length == 0) {
        	nodes = new SparkNode[]{cluster.getSpec().getMaster(), cluster.getSpec().getWorker()};
        }
    	
    	// collect master and worker nodes
    	List<Pod> pods = new ArrayList<Pod>();
    	for(SparkNode node : nodes) {
    		pods.addAll(getPodsByNode(cluster, node));
    	}
    	// delete pods
    	for(Pod pod : pods) {
    		// delete from cluster
	        client.pods()
	        	.inNamespace(cluster.getMetadata().getNamespace())
	        	.withName(pod.getMetadata().getName())
	        	.delete();
	        // add to deleted list
	        deletedPods.add(pod	);
    	}
		logger.debug(String.format("[%s] - deleted %d pod(s): %s", 
				state, deletedPods.size(), podListToDebug(deletedPods)));
    	return deletedPods; 
    }
    
    /**
     * Return number of pods for given nodes - Terminating is excluded
     * @param cluster - current spark cluster
     * @param nodes - master or worker node
     * @return list of pods belonging to the given node
     */
    public List<Pod> getPodsByNode(SparkCluster cluster, SparkNode... nodes) {
        List<Pod> podList = new ArrayList<>();
    	
        // if nodes are null take all
        if(nodes == null || nodes.length == 0) {
        	nodes = new SparkNode[]{cluster.getSpec().getMaster(), cluster.getSpec().getWorker()};
        }
        
    	for(SparkNode node : nodes) {
	        String nodeName = createPodName(cluster, node);
	        
	        for (Pod pod : podLister.list()) {
	        	// filter for pods not belonging to cluster
	        	if(podInCluster(pod) == null) {
	        		continue;
	        	}
	        	// filter for terminating pods
	        	if(pod.getMetadata().getDeletionTimestamp() != null) {
	        		continue;
	        	}
	        	if (pod.getMetadata().getName().contains(nodeName)) {
	        		// TODO: Filter PodStatus: Running...Failure etc.
                    podList.add(pod);
	            }
	        }
    	}
        return podList;
    }
    
    /**
     * Create pod name schema
     * @param cluster - current spark cluster
     * @param node - master or worker node
     * @return pod name
     */
    private String createPodName(SparkCluster cluster, SparkNode node) {
    	return cluster.getMetadata().getName() + "-" + node.getPodTypeName() + "-";
    }

    /**
     * Check incoming pods for owner reference of SparkCluster and add to blocking queue
     * @param pod - kubernetes pod
     */
    private void handlePodObject(Pod pod) {
    	SparkCluster cluster = podInCluster(pod);
    	if(cluster != null) {
    		enqueueCrd(cluster);
    	}
	}
    
    /**
     * Return the owner reference of that specific pod if available
     * @param pod - fabric8 Pod
     * @return pod owner reference
     */
    private OwnerReference getControllerOf(Pod pod) {
        List<OwnerReference> ownerReferences = pod.getMetadata().getOwnerReferences();
        for (OwnerReference ownerReference : ownerReferences) {
            if (ownerReference.getController().equals(Boolean.TRUE)) {
                return ownerReference;
            }
        }
        return null;
    }
    
    /**
     * Check if pod belongs to cluster
     * @param pod - pod to be checked for owner reference
     * @return SparkCluster the pod belongs to, otherwise null
     */
    private SparkCluster podInCluster(Pod pod) {
    	SparkCluster cluster = null;
    	OwnerReference ownerReference = getControllerOf(pod);
        Objects.requireNonNull(ownerReference);
        // check if pod belongs to spark cluster
        String sparkClusterKind = getCrdContext(crdMetadata).getKind();
        if (ownerReference.getKind().equalsIgnoreCase(sparkClusterKind)) {
        	cluster = crdLister.get(ownerReference.getName());
        }
        return cluster; 
    }
    
    /**
     * Create config map content for master / worker
     * @param cluster - spark cluster
     * @param node - spark master / worker
     */
    public void createConfigMap(SparkCluster cluster, SparkNode node) {
    	String cmName = createPodName(cluster, node) + "cm";
    	
        Resource<ConfigMap,DoneableConfigMap> configMapResource = client
        	.configMaps()
        	.inNamespace(cluster.getMetadata().getNamespace())
        	.withName(cmName);
        //
        // create entry for spark-env.sh
        //
        StringBuffer sbEnv = new StringBuffer();
        // only worker has required information to be set
        SparkNode worker = cluster.getSpec().getWorker();
        Map<String,String> cmFiles = new HashMap<String,String>();
        // all known data in yaml and pojo
        addToSparkEnv(sbEnv, SparkConfig.SPARK_WORKER_CORES, worker.getCores());
        addToSparkEnv(sbEnv, SparkConfig.SPARK_WORKER_MEMORY, worker.getMemory());
        cmFiles.put("spark-env.sh", sbEnv.toString());
        //
        // create entry for spark-defaults.conf
        //
        StringBuffer sbConf = new StringBuffer();
        addToSparkConfig(node.getSparkConfiguration(), sbConf);
        cmFiles.put("spark-defaults.conf", sbConf.toString());
        // create config map
        configMapResource.createOrReplace(new ConfigMapBuilder()
        	.withNewMetadata()
            	.withName(cmName)
            .endMetadata()
            .addToData(cmFiles)
            .build());
    }
    
    /**
     * Delete config map content for master / worker
     * @param cluster - spark cluster
     * @param node - spark master / worker
     */
    public void deleteConfigMap(SparkCluster cluster, SparkNode node) {
    	String cmName = createPodName(cluster, node) + "cm";
    	
        Resource<ConfigMap,DoneableConfigMap> configMapResource = client
        	.configMaps()
        	.inNamespace(cluster.getMetadata().getNamespace())
        	.withName(cmName);

        // delete config map
        configMapResource.delete();
    }
    
    /**
     * Get config map content for master / worker
     * @param cluster - spark cluster
     * @param node - spark master / worker
     */
    public ConfigMap getConfigMap(SparkCluster cluster, SparkNode node) {
    	String cmName = createPodName(cluster, node) + "cm";
    	
        Resource<ConfigMap,DoneableConfigMap> configMapResource = client
        	.configMaps()
        	.inNamespace(cluster.getMetadata().getNamespace())
        	.withName(cmName);

        // delete config map
        return configMapResource.get();
    }
    
    /**
     * Add spark environment variables to stringbuffer for spark-env.sh configuration
     * @param sb - string buffer to add to
     * @param config - key
     * @param value - value
     */
    private void addToSparkEnv(StringBuffer sb, SparkConfig config, String value) {
        if(value != null && !value.isEmpty()) {
        	sb.append(config.getEnv() + "=" + value + "\n");
        }
    }
    
    /**
     * Add to string buffer for spark configuration (spark-default.conf) in config map 
     * @param sparkConfiguration - spark config given in specs
     * @param sb - string buffer to add to
     */
    private void addToSparkConfig(List<EnvVar> sparkConfiguration, StringBuffer sb) {
    	for(EnvVar var: sparkConfiguration) {
    		String name = var.getName();
    		String value= var.getValue();
    		if(name != null && !name.isEmpty() && value != null && !value.isEmpty()) {
    			sb.append(name + " " + value + "\n");	
    		}
    	}
    }
    
    /**
     * Create pod for spark node
     * @param cluster - spark cluster
     * @param node - master or worker node
     * @return pod create from specs
     */
	private Pod createNewPod(SparkCluster cluster, SparkNode node) {
		String cmName = createPodName(cluster, node) + "cm";
		ConfigMapVolumeSource cms = new ConfigMapVolumeSourceBuilder().withName(cmName).build();
		Volume vol = new VolumeBuilder().withName(cmName).withConfigMap(cms).build();

        return new PodBuilder()
            .withNewMetadata()
              .withGenerateName(createPodName(cluster, node))
              .withNamespace(cluster.getMetadata().getNamespace())
              .addNewOwnerReference()
                  .withController(true)
                  .withKind(cluster.getKind())
                  .withApiVersion(cluster.getApiVersion())
                  .withName(cluster.getMetadata().getName())
                  .withNewUid(cluster.getMetadata().getUid())
              .endOwnerReference()
            .endMetadata()
            .withNewSpec()
                .withTolerations(node.getTolerations())
                // TODO: check for null / zero elements
                .withNodeSelector(node.getSelectors().get(0).getSelector().getMatchLabels())
                .withVolumes(vol)
	                .addNewContainer()
	                	//TODO: no ":" etc in withName
		            	.withName("spark-3-0-1")
		            	.withImage(cluster.getSpec().getImage())
		            	.withCommand(node.getCommands())
		            	.withArgs(node.getArgs())
		                .addNewVolumeMount()
		                	// TODO: replace hardcoded
		                  	.withMountPath("conf")
		                  	.withName(cmName)
		                .endVolumeMount()
		                .withEnv(node.getEnv())
	                .endContainer()
            .endSpec()
            .build();
    }
	
	/**
	 * Check if all given pods have status "Running"
	 * @param pods - list of pods
	 * @param status - PodStatus to compare to
	 * @return true if all pods have status from Podstatus
	 */
    public boolean allPodsHaveStatus(List<Pod> pods, PodState status) {
		boolean result = true;
    	for(Pod pod : pods) {
    		if(!pod.getStatus().getPhase().equals(status.toString())) {
    			result = false;
    		}
    	}
    	return result;
    }
    
    /**
     * Return difference between cluster specification and current cluster state
     * @param pods
     * @param cluster
     * @param node
     * @return 
     * = 0 if specification equals state -> no operation
     * < 0 if state greater than specification -> delete pods
     * > 0 if state smaller specification -> create pods
     */
    public int getPodSpecToClusterDifference(SparkNode node, List<Pod> pods) {
    	return node.getInstances() - pods.size();
    }
    
    /**
     * Extract leader node name from pods
     * @param pods - list of master pods
     * @return null or pod.spec.nodeName if available
     */
    public List<String> getHostNames(List<Pod> pods) {
    	// TODO: determine master leader
    	List<String> nodeNames = new ArrayList<String>();
    	for(Pod pod : pods) {
    		String nodeName = pod.getSpec().getNodeName();
    		if(nodeName != null && !nodeName.isEmpty()) {
    			nodeNames.add(nodeName);
    		}
    	}
    	return nodeNames;
    }
	
    /**
     * Adapt worker starting command with master node names as argument
     * @param cluster - spark cluster
     * @param masterNodeNames - list of available master nodes
     */
    public void adaptWorkerCommand(SparkCluster cluster, List<String> masterNodeNames) {
    	// adapt command in workers
    	List<String> commands = cluster.getSpec().getWorker().getCommands();

    	if(commands.size() == 1) {
    		String port = "7077";
    		// if different port in env
    		for(EnvVar var : cluster.getSpec().getWorker().getEnv() ) {
    			if(var.getName().equals("SPARK_MASTER_PORT")) {
    				port = var.getValue();
    			}
    		}
    		
    		StringBuffer sb = new StringBuffer();
    		sb.append("spark://");
    		for(int i = 0; i < masterNodeNames.size(); i++) {
    			sb.append(masterNodeNames.get(i) + ":" + port);
    			if(i < masterNodeNames.size() - 1) sb.append(",");
    		}
    		
    		String masterUrl = sb.toString();
    		commands.add(masterUrl);
			// TODO: dont use cluster state
    		logger.debug(String.format("[%s] - set worker MASTER_URL to: %s", clusterStateMachine.getState().name(), masterUrl));
    	}
    }
    
    /**
     * Helper method to print pod lists
     * @param pods - list of pods
     * @return pod metadata.name
     */
	public List<String> podListToDebug(List<Pod> pods) {
		List<String> output = new ArrayList<String>();
		pods.forEach((n) -> output.add(n.getMetadata().getName()));
		return output;
	}
}
