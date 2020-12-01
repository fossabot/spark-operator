package com.stackable.spark.operator.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.SparkClusterDoneable;
import com.stackable.spark.operator.cluster.SparkClusterList;
import com.stackable.spark.operator.cluster.crd.SparkNode;
import com.stackable.spark.operator.common.type.PodStatus;
import com.stackable.spark.operator.common.type.SparkClusterState;
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
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Lister;

/**
 * The SparkClusterController is responsible for installing the spark master and workers.
 * Scale up and down to the instances required in the specification.
 */
public class SparkClusterController extends AbstractCrdController<SparkCluster, SparkClusterList, SparkClusterDoneable> {
	
    public static final Logger logger = Logger.getLogger(SparkClusterController.class.getName());

    public static final String SPARK_CLUSTER_KIND 	= "SparkCluster";
    public static final String APP_LABEL 			= "cluster";

    private SharedIndexInformer<Pod> podInformer;
    private Lister<Pod> podLister;
    
    private SparkClusterState clusterState;
    
	public SparkClusterController(
		KubernetesClient client,
		SharedInformerFactory informerFactory,
		String namespace,
		String crdPath,
		Long resyncCycle) {

		super(client, informerFactory, namespace, crdPath, resyncCycle);
		
        this.podInformer = informerFactory.sharedIndexInformerFor(Pod.class, PodList.class, resyncCycle);
        this.podLister = new Lister<>(podInformer.getIndexer(), namespace);

        this.clusterState = SparkClusterState.INITIAL;
        // register pods
        registerPodEventHandler();
    }
	
    protected CustomResourceDefinitionContext getCrdContext() {
        return new CustomResourceDefinitionContext.Builder()
            .withVersion("v1")
            .withScope("Namespaced")
            .withGroup("spark.stackable.de")
            .withPlural("sparkclusters")
            .build();
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

	/**
	 * Cluster state machine:
	 * 
	 * 		INITIAL
	 * 			|
	 * 			v
	 *  	CREATE_SPARK_MASTER	<-------<---+
	 *  		|						|	|
	 *  		|						|	|
	 *  		v						|	|
	 *  	WAIT_FOR_HOST_NAME ---------^	|
	 *  		|						|	|
	 *  		|						|	|
	 *  		v						|	|
	 *  	WAIT_FOR_MASTER_RUNNING	----+	|
	 *  		|							|
	 *  		|							|
	 *  		v							|
	 *  	CREATE_SPARK_WORKER	<>------+---+
	 *  		|						^	|
	 *  		|						|	|
	 *  		v						|	|
	 *  	WAIT_FOR_WORKERS_RUNNING ---+---^
	 *  		|							|
	 *  		|							|
	 *  		v							|
	 *  	RECONCILE ----------------------+
	 */
	@Override
	protected void process(SparkCluster cluster) {
		switch(clusterState) {
		/**
		 * INITIAL:
		 * Initialize and clear resources
		 */
		case INITIAL: {
			logger.debug(String.format("[%s]", clusterState.toString()));
			clusterState = SparkClusterState.CREATE_SPARK_MASTER;
			
			// no break required
			// break;
		}
		/**
		 * CREATE_SPARK_MASTER:
		 * Spawn master pods given by specification
		 */
		case CREATE_SPARK_MASTER: {
			if(reconcile(cluster) == SparkClusterState.CREATE_SPARK_WORKER) {
				// ignore
			};
        	// create master instances
			SparkNode master = cluster.getSpec().getMaster();
			List<Pod> masterPods = getPodsByNode(cluster, master);
			
        	createPods(masterPods, cluster, master);
        	
	        clusterState = SparkClusterState.WAIT_FOR_MASTER_HOST_NAME;
        	// no break required
			//break;
		}
	    /**
	     * WAIT_FOR_MASTER_HOST_NAME: 
	     * after master is created, wait for agent to set the dedicated host name and use for workers 
	     * ==> create config map
	     */
		case WAIT_FOR_MASTER_HOST_NAME: {
			// TODO: multiple master?
			if(reconcile(cluster) == SparkClusterState.CREATE_SPARK_WORKER) {
				// ignore
			};
        	
        	SparkNode master = cluster.getSpec().getMaster();
        	List<Pod> masterPods = getPodsByNode(cluster, master);
			// check for host name -> wait if not available
        	// TODO who is leader?
        	String nodeName = getNodeNameFromLeader(masterPods);
        	if(nodeName == null || nodeName.isEmpty()) {
        		break;
        	}
        	// create master config map 
        	createConfigMap(cluster, master);
    		
   			clusterState = SparkClusterState.WAIT_FOR_MASTER_RUNNING;
   			// no break required
			// break;
		}
	    /**
	     * WAIT_FOR_MASTER_RUNNING
	     * Wait before the master is configured and running before spawning workers
	     */
		case WAIT_FOR_MASTER_RUNNING: {
			if(reconcile(cluster) == SparkClusterState.CREATE_SPARK_WORKER) {
				// ignore
			};

			// TODO: multiple master?
			SparkNode master = cluster.getSpec().getMaster();
			List<Pod> masterPods = getPodsByNode(cluster,master);
			
        	// wait for running
        	if(!allPodsHaveStatus(masterPods, PodStatus.RUNNING)) {
        		break;
        	}
        	
        	clusterState = SparkClusterState.CREATE_SPARK_WORKER;
        	// no break required
			//break;
		}
	    /**
	     * CREATE_SPARK_WORKER:
	     * Spawn worker pods given by specification
	     */
		case CREATE_SPARK_WORKER: {
			if(reconcile(cluster) != SparkClusterState.CREATE_SPARK_WORKER) {
				break;
			};
        	
			SparkNode master = cluster.getSpec().getMaster();
			List<Pod> masterPods = getPodsByNode(cluster, master);
			SparkNode worker = cluster.getSpec().getWorker();
			List<Pod> workerPods = getPodsByNode(cluster, worker);
			
        	// check host name
        	String leaderHostName = getNodeNameFromLeader(masterPods);
        	if( leaderHostName == null || leaderHostName.isEmpty()) {
        		break;
        	}
        	// adapt command in workers
        	adaptWorkerCommand(cluster, leaderHostName);

        	// spin up workers
        	workerPods = createPods(workerPods, cluster, worker);
        	// create config map        	
        	createConfigMap(cluster, worker);
        	
        	clusterState = SparkClusterState.WAIT_FOR_WORKERS_RUNNING;
        	// no break needed
			// break;
		}
	    /**
	     * WAIT_FOR_WORKERS_RUNNING
	     * Wait for all workers to run
	     */
		case WAIT_FOR_WORKERS_RUNNING: {
			if(reconcile(cluster) != SparkClusterState.WAIT_FOR_WORKERS_RUNNING) {
				break;
			};
			
        	List<Pod> workerPods = getPodsByNode(cluster, cluster.getSpec().getWorker());

        	// wait for running
        	if(!allPodsHaveStatus(workerPods, PodStatus.RUNNING)) {
        		break;
        	}
        	
        	clusterState = SparkClusterState.RECONCILE;
        	// no break needed
			break;
		}
	    /**
	     * RECONCILE:
	     * Watch the cluster state and act if spec != state
	     */
		case RECONCILE: {
			clusterState = reconcile(cluster); 
			break;
		}}
	}
	
	@Override
	protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced() || !podInformer.hasSynced());
		logger.info("SparkCluster informer and pod informer initialized ... waiting for changes");
	}
    
	/**
	 * Create pods with regard to spec and current state
	 * @param pods - list of available pods belonging to the given node
	 * @param cluster - current spark cluster
	 * @param node - master or worker node
	 * @return list of created pods
	 */
    private List<Pod> createPods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    	List<Pod> createdPods = new ArrayList<Pod>();
        // Compare with desired state (spec.master.node.instances)
        // If less then create new pods
        if (pods.size() < node.getInstances()) {
            for (int index = 0; index < node.getInstances() - pods.size(); index++) {
                Pod pod = client.pods().inNamespace(cluster.getMetadata().getNamespace()).create(createNewPod(cluster, node));
                createdPods.add(pod);
            }
        }
		logger.debug(String.format("[%s] - created %d %s pod(s): %s", 
			clusterState.toString(), createdPods.size(), node.getPodTypeName(), podListToDebug(createdPods)));
        return createdPods;
    }
    
	/**
	 * Delete pods with regard to spec and current state
	 * @param pods - list of available pods belonging to the given node
	 * @param cluster - current spark cluster
	 * @param node - master or worker node
	 * @return list of deleted pods
	 */
    private List<Pod> deletePods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
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
				clusterState.toString(), deletedPods.size(), node.getPodTypeName(), podListToDebug(deletedPods)));
        return deletedPods;
    }
    
    /**
     * Return number of pods for given nodes - Terminating is excluded
     * @param cluster - current spark cluster
     * @param nodes - master or worker node
     * @return list of pods belonging to the given node
     */
    private List<Pod> getPodsByNode(SparkCluster cluster, SparkNode... nodes) {
        List<Pod> podList = new ArrayList<>();
    	
    	for(SparkNode node : nodes) {
	        String nodeName = createPodName(cluster, node);
	        
	        for (Pod pod : podLister.list()) {
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
     * Check incoming pods for owner reference spark cluster and add to blocking queue
     * @param pod - kubernetes pod
     */
    private void handlePodObject(Pod pod) {
        OwnerReference ownerReference = getControllerOf(pod);
        Objects.requireNonNull(ownerReference);
        // check if pod belongs to spark cluster
        if (!ownerReference.getKind().equalsIgnoreCase(SPARK_CLUSTER_KIND)) {
            return;
        }
        
        SparkCluster cluster = crdLister.get(ownerReference.getName());
        
        if (cluster == null) {
        	return;
        }

        enqueueCrd(cluster);
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
    
    private void createConfigMap(SparkCluster cluster, SparkNode node) {
    	String cmName = createPodName(cluster, node) + "cm";
    	
        Resource<ConfigMap,DoneableConfigMap> configMapResource = client
        	.configMaps()
        	.inNamespace(cluster.getMetadata().getNamespace())
        	.withName(cmName);

        // create entry for spark-env.sh
        StringBuffer sb = new StringBuffer();
        // only worker has required information to be set
        SparkNode worker = cluster.getSpec().getWorker();
        Map<String,String> configFiles = new HashMap<String,String>();
        // all known data in yaml
        addToConfigFiles(configFiles, sb, SparkConfig.SPARK_WORKER_CORES, worker.getCores());
        addToConfigFiles(configFiles, sb, SparkConfig.SPARK_WORKER_MEMORY, worker.getMemory());
        
        configFiles.put("spark-env.sh", sb.toString());
        
        configMapResource.createOrReplace(new ConfigMapBuilder()
        	.withNewMetadata()
            	.withName(cmName)
            .endMetadata()
            .addToData(configFiles)
            .build());
    }
    
    private void addToConfigFiles(Map<String,String> configFiles, StringBuffer sb, SparkConfig config, String value) {
        if(value != null && !value.isEmpty()) {
        	sb.append(config.toString() + "=" + value + "\n");
        }
    }
    
	private Pod createNewPod(SparkCluster cluster, SparkNode node) {
		// TODO: replace hardcoded
		String cmName = createPodName(cluster, node) + "cm";
		ConfigMapVolumeSource cms = new ConfigMapVolumeSourceBuilder().withName(cmName).build();
		Volume vol = new VolumeBuilder().withName(cmName).withConfigMap(cms).build();
		List<Toleration> tolerations = new ArrayList<Toleration>();
		tolerations.add( new TolerationBuilder().withNewEffect("NoSchedule").withKey("kubernetes.io/arch").withOperator("Equal").withValue("stackable-linux").build());
		tolerations.add( new TolerationBuilder().withNewEffect("NoExecute").withKey("kubernetes.io/arch").withOperator("Equal").withValue("stackable-linux").build());
		tolerations.add( new TolerationBuilder().withNewEffect("NoExecute").withKey("node.kubernetes.io/not-ready").withOperator("Exists").withTolerationSeconds(300L).build());
		tolerations.add( new TolerationBuilder().withNewEffect("NoSchedule").withKey("node.kubernetes.io/unreachable").withOperator("Exists").build());
		tolerations.add( new TolerationBuilder().withNewEffect("NoExecute").withKey("node.kubernetes.io/unreachable").withOperator("Exists").withTolerationSeconds(300L).build());

        return new PodBuilder()
                .withNewMetadata()
                  .withGenerateName(createPodName(cluster, node))
                  .withNamespace(cluster.getMetadata().getNamespace())
                  .withLabels(Collections.singletonMap(APP_LABEL, cluster.getMetadata().getName()))
                  .addNewOwnerReference()
                  .withController(true)
                  .withKind(cluster.getKind())
                  .withApiVersion(cluster.getApiVersion())
                  .withName(cluster.getMetadata().getName())
                  .withNewUid(cluster.getMetadata().getUid())
                  .endOwnerReference()
                .endMetadata()
                .withNewSpec()
                .withTolerations(tolerations)
                // TODO: check for null / zero elements
                .withNodeSelector(node.getSelectors().get(0).getSelector().getMatchLabels())
                .withVolumes(vol)
                .addNewContainer()
                	//TODO: no ":" etc in withName
	            	.withName("spark3")
	            	.withImage(cluster.getSpec().getImage())
	            	.withCommand(node.getCommand())
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
	 * Reconcile the spark cluster. Compare current with desired state and adapt.
	 * @param controller - current spark cluster controller
	 * @param cluster - current spark cluster 
	 * @param nodes - master/worker to reconcile
	 * @return 
	 * RECONCILE if nothing to do; 
	 * CREATE_SPARK_MASTER if #masters < spec;
	 * CREATE_SPARK_WORKER if #workers < spec;
	 */
    private SparkClusterState reconcile(SparkCluster cluster, SparkNode... nodes) {
    	SparkClusterState state = clusterState;
    	SparkNode master = cluster.getSpec().getMaster();
    	SparkNode worker = cluster.getSpec().getWorker();
    	List<Pod> masterPods = getPodsByNode(cluster, master);
    	List<Pod> workerPods = getPodsByNode(cluster, worker);
    	
		logger.debug(String.format("[%s] - %s [%d / %d] | %s [%d / %d]", 
						clusterState.toString(),
						master.getPodTypeName(), masterPods.size(), master.getInstances(),
						worker.getPodTypeName(), workerPods.size(), worker.getInstances()
					));
    	
		// master missing
        if(getPodSpecToClusterDifference(master, masterPods) > 0) {
        	state = SparkClusterState.CREATE_SPARK_MASTER;
        }
        // worker missing && master pods equal spec
        else if(getPodSpecToClusterDifference(worker, workerPods) > 0 &&
        		masterPods.size() == master.getInstances()) {
        	state = SparkClusterState.CREATE_SPARK_WORKER;
        }
        
        // delete master pods
        // TODO: leader?
        if(getPodSpecToClusterDifference(master, masterPods) < 0) {
        	deletePods(masterPods, cluster, master);
        }
        
        // delete worker pods
        if(getPodSpecToClusterDifference(worker, workerPods) < 0) {
        	deletePods(workerPods, cluster, worker);
        }
        
        return state;
    }
    
	/**
	 * Check if all given pods have status "Running"
	 * @param pods - list of pods
	 * @param status - PodStatus to compare to
	 * @return true if all pods have status from Podstatus
	 */
    private boolean allPodsHaveStatus(List<Pod> pods, PodStatus status) {
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
    private int getPodSpecToClusterDifference(SparkNode node, List<Pod> pods) {
    	return node.getInstances() - pods.size();
    }
    
    /**
     * Extract leader node name from pods
     * @param pods - list of master pods
     * @return null or pod.spec.nodeName if available
     */
    private String getNodeNameFromLeader(List<Pod> pods) {
    	// TODO: determine master leader
    	for(Pod pod : pods) {
    		String nodeName = pod.getSpec().getNodeName();
    		if(nodeName != null && !nodeName.isEmpty()) {
    			logger.debug(String.format("[%s] - got nodeName: %s", clusterState.toString(), nodeName));
    			return nodeName;
    		}
    	}
    	return null;
    }
	
    private void adaptWorkerCommand(SparkCluster cluster, String leaderHostName) {
    	// adapt command in workers
    	List<String> commands = cluster.getSpec().getWorker().getCommand();
    	// TODO: start-worker.sh? - adapt port
    	if(commands.size() == 1) {
    		String port = "7077";
    		// if different port in env
    		for(EnvVar var : cluster.getSpec().getWorker().getEnv() ) {
    			if(var.getName().equals("SPARK_MASTER_PORT")) {
    				port = var.getValue();
    			}
    		}
    		
    		String masterUrl = "spark://" + leaderHostName + ":" + port;
    		commands.add(masterUrl);
    		
    		logger.debug(String.format("[%s] - set worker MASTER_URL to: %s", clusterState.toString(), masterUrl));
    	}
    }
    /**
     * Helper method to print pod lists
     * @param pods - list of pods
     * @return pod metadata.name
     */
	private List<String> podListToDebug(List<Pod> pods) {
		List<String> output = new ArrayList<String>();
		pods.forEach((n) -> output.add(n.getMetadata().getName()));
		return output;
	}
	
}
