package com.stackable.spark.operator.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatus;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatusCommand;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatusImage;
import com.stackable.spark.operator.cluster.crd.SparkNode;
import com.stackable.spark.operator.common.state.PodState;
import com.stackable.spark.operator.common.state.SparkClusterState;
import com.stackable.spark.operator.common.state.SparkSystemdActionState;
import com.stackable.spark.operator.common.state.SparkSystemdState;
import com.stackable.spark.operator.common.type.SparkConfig;
import com.stackable.spark.operator.common.type.SparkSystemdAction;

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
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;

/**
 * The SparkClusterController is responsible for installing the spark master and workers.
 * Scale up and down to the instances required in the specification.
 * Offer systemd functionality for start, stop and restart the cluster
 */
public class SparkClusterController extends AbstractCrdController<SparkCluster> {
	
    private static final Logger logger = Logger.getLogger(SparkClusterController.class.getName());

    private SharedIndexInformer<Pod> podInformer;
    private Lister<Pod> podLister;
    
    private SparkSystemdState systemdState;
    private SparkClusterState clusterState;
    
	public SparkClusterController(String crdPath, Long resyncCycle) {
		super(crdPath, resyncCycle);
		
        this.podInformer = informerFactory.sharedIndexInformerFor(Pod.class, PodList.class, resyncCycle);
        this.podLister = new Lister<>(podInformer.getIndexer(), namespace);

        this.systemdState = SparkSystemdState.SYSTEMD_INITIAL;
        this.clusterState = SparkClusterState.INITIAL;
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
		if(processSystemdStateMachine(cluster)) {
			return;
		}
		// only go for cluster state machine if no systemd action is currently running
		processClusterStateMachine(cluster);
	}
	
	/**
	 * Systemd state machine:
	 * 			|	
	 * 			+<----------------------------------+
	 * 			v									^
	 * 		SYSTEMD_INITIAL---------------------+	|
	 * 			|								^	|
	 * 			v								|	|
	 * 	 	SYSTEMD_UPDATE						|	|
	 * 			|								|	|
	 * 			v								|	|
	 *  	SYSTEMD_WAIT_FOR_IMAGE_UPDATED------+-->+
	 *		  	|								|	^
	 *			v								|	|
	 * 		SYSTEMD_RESTART	<-------------------+	|
	 * 			|									|
	 * 			v									|
 	 *  	SYSTEMD_WAIT_FOR_JOBS_FINISHED			|
 	 *  		|									|
 	 *  		v									|
	 * 		SYSTEMD_WAIT_FOR_PODS_DELETED-----------+
	 */
	private boolean processSystemdStateMachine(SparkCluster cluster) {
		boolean systemdInProgress = false;
		// check if systemd required -> check status for staged or running command
		if(cluster.getStatus() == null || cluster.getStatus().getSystemd() == null) { 
			return systemdInProgress;
		}
		// command running: not status not null, command not null, action not finished
		if(cluster.getStatus().getSystemd().getRunningCommand() != null && 
			cluster.getStatus().getSystemd().getRunningCommand().getCommand() != null &&
			!cluster.getStatus().getSystemd().getRunningCommand().getStatus().equals(SparkSystemdActionState.FINISHED.toString())) {
			systemdInProgress = true;
		}
		// command staged
		else if(cluster.getStatus().getSystemd().getStagedCommands().size() != 0) {
			SparkSystemdAction action = getSystemdStagedCommand(cluster.getStatus().getSystemd().getStagedCommands());
			// check for state action
			if(action == SparkSystemdAction.NONE) {
				logger.warn("unidentified systemd action - continue with reconcilation");
				return systemdInProgress;
			}
			else if(action == SparkSystemdAction.STOP) {
				systemdInProgress = true;
				systemdState = SparkSystemdState.SYSTEMD_STOP;
			}
			else if(action == SparkSystemdAction.START) {
				systemdInProgress = false;
				systemdState = SparkSystemdState.SYSTEMD_START;
			}
			else if(action == SparkSystemdAction.RESTART) {
				systemdInProgress = true;
				systemdState = SparkSystemdState.SYSTEMD_RESTART;
			}
			else if(action == SparkSystemdAction.UPDATE) {
				systemdInProgress = true;
				systemdState = SparkSystemdState.SYSTEMD_UPDATE;
			}
		}
		
		switch(systemdState) {
		case SYSTEMD_INITIAL: {
			break;
		}
		case SYSTEMD_STOP: {
			logger.debug(String.format("[%s] - cluster stopped", systemdState.toString()));
			// TODO: remove command etc - wait until start / restart commands arrive
			//deletePods(cluster);
			break;
		}
		case SYSTEMD_START: {
			logger.debug(String.format("[%s] - cluster started", systemdState.toString()));
			// TODO: remove command etc, go for cluster reconcile
			// get stages command and remove from list
			//String stagedCommand = cluster.getStatus().getSystemd().getStagedCommands().remove(0);
			// jump out
			//systemdInProgress = false;
			break;
		}
		case SYSTEMD_UPDATE: {
			logger.debug(String.format("[%s]", systemdState.toString()));
			// TODO: wait for jobs to be finished
			// no new image arrived
			if(cluster.getStatus().getImage().getName().equals(cluster.getSpec().getImage())) {
				logger.warn("systemd update called but no new image(" + cluster.getSpec().getImage() + ") available -> abort!");
				// reset state
				systemdState = SparkSystemdState.SYSTEMD_INITIAL;
				// TODO: remove systemd? 
				break;
			}
			systemdState = SparkSystemdState.SYSTEMD_WAIT_FOR_IMAGE_UPDATED;
		}
		case SYSTEMD_WAIT_FOR_IMAGE_UPDATED: {
			logger.debug(String.format("[%s]", systemdState.toString()));
			systemdState = SparkSystemdState.SYSTEMD_RESTART;
			
		}
		case SYSTEMD_RESTART: {
			logger.debug(String.format("[%s]", systemdState.toString()));
			// TODO: wait for jobs to be finished
			
			// get stages command and remove from list
			String stagedCommand = cluster.getStatus().getSystemd().getStagedCommands().remove(0);
			// set staged to running
			cluster.getStatus().getSystemd().setRunningCommands(
				new SparkClusterStatusCommand.Builder()
					.withCommand(stagedCommand)
					.withStartedAt(String.valueOf(System.currentTimeMillis()))
					.withStatus(SparkSystemdActionState.RUNNING.toString())
					.build()
					);
			// update
			crdClient.updateStatus(cluster);
			
			deletePods(cluster);
			systemdState = SparkSystemdState.SYSTEMD_WAIT_FOR_JOBS_FINISHED;
			break;
		}
		case SYSTEMD_WAIT_FOR_JOBS_FINISHED: {
			//logger.debug(String.format("[%s]", systemdState.toString()));
			systemdState = SparkSystemdState.SYSTEMD_WAIT_FOR_PODS_DELETED;
		}
		case SYSTEMD_WAIT_FOR_PODS_DELETED: {
			logger.debug(String.format("[%s]", systemdState.toString()));
			// get pods and wait for all deleted
			List<Pod> pods = getPodsByNode(cluster, (SparkNode[])null);
			// still pods available?
			if(pods.size() > 0) {
				// stop and wait
				break;
			}
			// set status running command to finished, set finished timestamp and update 
			SparkClusterStatusCommand runningCommand = cluster.getStatus().getSystemd().getRunningCommand();
			runningCommand.setStatus(SparkSystemdActionState.FINISHED.toString());
			runningCommand.setFinishedAt(String.valueOf(System.currentTimeMillis()));
			cluster.getStatus().getSystemd().setRunningCommands(runningCommand);
			crdClient.updateStatus(cluster);
		
			// reset systemd state
			systemdState = SparkSystemdState.SYSTEMD_INITIAL;
			// reset spark cluster state 
			clusterState = SparkClusterState.INITIAL;
		}}
		
		return systemdInProgress;
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
	protected void processClusterStateMachine(SparkCluster cluster) {
		// validate state 
		SparkClusterState validatedClusterState = reconcile(cluster);
		// always go for INITIAL first and wait for master states
		if(	clusterState != SparkClusterState.INITIAL &&
			clusterState != SparkClusterState.CREATE_SPARK_MASTER &&
			clusterState != SparkClusterState.WAIT_FOR_MASTER_HOST_NAME &&
			clusterState != SparkClusterState.WAIT_FOR_MASTER_RUNNING ) {
			clusterState = validatedClusterState;
		}
		
		switch(clusterState) {
		/**
		 * INITIAL:
		 * Initialize and clear resources
		 */
		case INITIAL: {
			clusterState = SparkClusterState.CREATE_SPARK_MASTER;
			
			// add spark image (deployedImage) to status for systemd update
			SparkClusterStatus status = cluster.getStatus();
			
			if(status == null) {
				status = new SparkClusterStatus();
			}
			
			status.setImage(
				new SparkClusterStatusImage(cluster.getSpec().getImage(), String.valueOf(System.currentTimeMillis()))
			);
			cluster.setStatus(status);
			// update status
			crdClient.updateStatus(cluster);
			// no break required
		}
		/**
		 * CREATE_SPARK_MASTER:
		 * Spawn master pods given by specification
		 */
		case CREATE_SPARK_MASTER: {
        	// create master instances
			SparkNode master = cluster.getSpec().getMaster();
			List<Pod> masterPods = getPodsByNode(cluster, master);
			
        	createPods(masterPods, cluster, master);
        	
	        clusterState = SparkClusterState.WAIT_FOR_MASTER_HOST_NAME;
			break;
		}
	    /**
	     * WAIT_FOR_MASTER_HOST_NAME: 
	     * after master is created, wait for agent to set the dedicated host name and use for workers 
	     * ==> create config map
	     */
		case WAIT_FOR_MASTER_HOST_NAME: {
        	SparkNode master = cluster.getSpec().getMaster();
        	List<Pod> masterPods = getPodsByNode(cluster, master);
			// check for host name -> wait if not available
        	// TODO who is leader?
        	List<String> nodeName = getMasterNodeNames(masterPods);
        	
        	if(nodeName.size() == 0) {
        		break;
        	}
        	
        	// create master config map 
        	createConfigMap(cluster, master);
    		
   			clusterState = SparkClusterState.WAIT_FOR_MASTER_RUNNING;
			break;
		}
	    /**
	     * WAIT_FOR_MASTER_RUNNING
	     * Wait before the master is configured and running before spawning workers
	     */
		case WAIT_FOR_MASTER_RUNNING: {
			// TODO: multiple master?
			SparkNode master = cluster.getSpec().getMaster();
			List<Pod> masterPods = getPodsByNode(cluster, master);
			
        	// wait for running
        	if(!allPodsHaveStatus(masterPods, PodState.RUNNING)) {
        		break;
        	}
        	
        	clusterState = SparkClusterState.CREATE_SPARK_WORKER;
        	// no break required
		}
	    /**
	     * CREATE_SPARK_WORKER:
	     * Spawn worker pods given by specification
	     */
		case CREATE_SPARK_WORKER: {
			SparkNode master = cluster.getSpec().getMaster();
			List<Pod> masterPods = getPodsByNode(cluster, master);
			SparkNode worker = cluster.getSpec().getWorker();
			List<Pod> workerPods = getPodsByNode(cluster, worker);
			
        	// check host name
        	List<String> masterNodeNames = getMasterNodeNames(masterPods);
        	
        	if( masterNodeNames.size() == 0) {
        		break;
        	}
        	// adapt command in workers
        	adaptWorkerCommand(cluster, masterNodeNames);

        	// spin up workers
        	workerPods = createPods(workerPods, cluster, worker);
        	// create config map        	
        	createConfigMap(cluster, worker);
        	
        	clusterState = SparkClusterState.WAIT_FOR_WORKERS_RUNNING;
			break;
		}
	    /**
	     * WAIT_FOR_WORKERS_RUNNING
	     * Wait for all workers to run
	     */
		case WAIT_FOR_WORKERS_RUNNING: {
        	List<Pod> workerPods = getPodsByNode(cluster, cluster.getSpec().getWorker());

        	// wait for running
        	if(!allPodsHaveStatus(workerPods, PodState.RUNNING)) {
        		break;
        	}
        	
        	clusterState = SparkClusterState.RECONCILE;
        	// no break needed
		}
	    /**
	     * RECONCILE:
	     * Watch the cluster state and act if spec != state
	     */
		case RECONCILE: {
			logger.debug(String.format("[%s]", clusterState.toString()));
			break;
		}}
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
	 * Delete pods with regard to spec and current state -> for reconcilation
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
     * Delete all node (master/worker) pods in cluster with no regard to spec -> systemd
     * @param cluster - cluster specification to retrieve all used pods
     * @return list of deleted pods
     */
    private List<Pod> deletePods(SparkCluster cluster, SparkNode ...nodes) {
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
				systemdState.toString(), deletedPods.size(), podListToDebug(deletedPods)));
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
     * Check incoming pods for owner reference spark cluster and add to blocking queue
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
    private void createConfigMap(SparkCluster cluster, SparkNode node) {
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
    private boolean allPodsHaveStatus(List<Pod> pods, PodState status) {
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
    private List<String> getMasterNodeNames(List<Pod> pods) {
    	// TODO: determine master leader
    	List<String> nodeNames = new ArrayList<String>();
    	for(Pod pod : pods) {
    		String nodeName = pod.getSpec().getNodeName();
    		if(nodeName != null && !nodeName.isEmpty()) {
    			logger.debug(String.format("[%s] - got nodeName: %s", clusterState.toString(), nodeName));
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
    private void adaptWorkerCommand(SparkCluster cluster, List<String> masterNodeNames) {
    	// adapt command in workers
    	List<String> commands = cluster.getSpec().getWorker().getCommands();
    	// TODO: start-worker.sh? - adapt port
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
	
	/**
	 * Retrieve first staged status command
	 * @param commands - list of staged commands
	 * @return SparkSystemAction in staged command (e.g. RESTART, UPDATE)
	 */
	private SparkSystemdAction getSystemdStagedCommand(List<String> commands) {
		if(commands.size() != 0) {
			String com = commands.get(0);
			
			for (SparkSystemdAction action : SparkSystemdAction.values()) { 
				if(action.toString().equals(com.toUpperCase()))
					return action;
			}
		}
		return SparkSystemdAction.NONE;
	}

}
