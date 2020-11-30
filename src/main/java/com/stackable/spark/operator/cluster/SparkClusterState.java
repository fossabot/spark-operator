package com.stackable.spark.operator.cluster;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.stackable.spark.operator.cluster.crd.SparkNode;
import com.stackable.spark.operator.cluster.crd.SparkNodeMaster;
import com.stackable.spark.operator.cluster.crd.SparkNodeWorker;
import com.stackable.spark.operator.common.type.PodStatus;
import com.stackable.spark.operator.controller.SparkClusterController;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;

public enum SparkClusterState {
	/*
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
	
	/**
	 * INITIAL:
	 * Initialize and clear resources
	 */
    INITIAL("INITIAL") {
        @Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	super.log(Level.INFO, "StateMachine started...");
        	// reset hostMap
        	controller.getHostNameMap().clear();
            return CREATE_SPARK_MASTER;
        }
    },
	/**
	 * CREATE_SPARK_MASTER:
	 * Spawn master pods given by specification
	 */
    CREATE_SPARK_MASTER("CREATE_SPARK_MASTER") {
        @Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	SparkNode master = cluster.getSpec().getMaster();
        	List<Pod> pods = controller.getPodsByNode(cluster, master);
        	
        	super.log(Level.INFO, String.format("%s [%d / %d]", master.getPodTypeName(), pods.size(), master.getInstances()));
        	
        	// create master instances
        	List<Pod> createdPods = controller.createPods(pods, cluster, master);
        	if(createdPods.size() > 0) {
        		List<String> output = new ArrayList<String>();
        		createdPods.forEach((n) -> output.add(n.getMetadata().getName()));
        		super.log(Level.INFO, "Created pods: " + output);
        	}
        	
            return WAIT_FOR_MASTER_HOST_NAME;
        }
    },
    /**
     * WAIT_FOR_MASTER_HOST_NAME: 
     * after master is created, wait for agent to set the dedicated host name and use for workers 
     * ==> create config map
     */
    WAIT_FOR_MASTER_HOST_NAME("WAIT_FOR_MASTER_HOST_NAME") {
        @Override
        public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	// check for master nodes
        	SparkClusterState scs = super.reconcile(controller, cluster, cluster.getSpec().getMaster());
        	if(scs != this) {
        		return scs;
        	}
        	
        	// TODO: multiple master?
        	List<Pod> masterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
        	
			// check for host name
        	String nodeName = masterPods.get(0).getSpec().getNodeName();
        	if( nodeName == null || nodeName.isEmpty()) {
        		return this;
        	}
        	
        	super.log(Level.INFO, "Received hostname: " + nodeName  + " for pod: " + masterPods.get(0).getMetadata().getName());
    		// save host name for workers
    		controller.addToHostMap(masterPods.get(0).getMetadata().getName(), nodeName);
    		// check if master pods available 
        	if(masterPods.isEmpty()) {
        		super.log(Level.INFO, "No master pod for config map creation available...");
        		return this;
        	}
        	// create config map
        	controller.createConfigMap(cluster, masterPods.get(0));
    		
   			return WAIT_FOR_MASTER_RUNNING;
        }
    },
    /**
     * WAIT_FOR_MASTER_RUNNING
     * Wait before the master is configured and running before spawning workers
     */
    WAIT_FOR_MASTER_RUNNING("WAIT_FOR_MASTER_RUNNING") {
        @Override
        public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	// check for master nodes
        	SparkClusterState scs = super.reconcile(controller, cluster, cluster.getSpec().getMaster());
        	if(scs != this) {
        		return scs;
        	}
        	
        	List<Pod> masterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
        	
        	// wait for running
        	if(super.allPodsHaveStatus(masterPods, PodStatus.RUNNING)) {
        		return CREATE_SPARK_WORKER;
        	}
        	
        	SparkNode master = cluster.getSpec().getMaster();
        	super.log(Level.INFO, String.format("%s [%d / %d]", master.getPodTypeName(), masterPods.size(), master.getInstances()));
        	
            return this;
        }
    },
    /**
     * CREATE_SPARK_WORKER:
     * Spawn worker pods given by specification
     */
    CREATE_SPARK_WORKER("CREATE_SPARK_WORKER") {
        @Override
        public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	// check for master nodes
        	SparkClusterState scs = super.reconcile(controller, cluster, cluster.getSpec().getMaster());
        	if(scs != this) {
        		return scs;
        	}
        	
        	List<Pod> masterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
        	// check host name
        	String masterHostName = controller.getHostNameMap().get(masterPods.get(0).getMetadata().getName());
        	if( masterHostName == null || masterHostName.isEmpty()) {
        		return WAIT_FOR_MASTER_HOST_NAME;
        	}
        	// TODO: copy cpu, memory etc to env variables
        	controller.addNodeConfToEnvVariables(cluster.getSpec().getWorker());
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
        		
        		String masterUrl = "spark://" + masterHostName + ":" + port;
        		commands.add(masterUrl);
        		super.log(Level.INFO, "Set worker MASTER_URL to: " + masterUrl);
        	}

        	// spin up workers
        	SparkNodeWorker worker = cluster.getSpec().getWorker();
        	List<Pod> workerPods = controller.getPodsByNode(cluster, worker);
        	
        	workerPods = controller.createPods(workerPods, cluster, worker);
        	
        	if(workerPods.size() > 0) {
        		List<String> output = new ArrayList<String>();
        		workerPods.forEach((n) -> output.add(n.getMetadata().getName()));
        		super.log(Level.INFO, "Created pods: " + output);
        	}
        	
        	// check if worker pods available
        	if(workerPods.isEmpty()) {
        		super.log(Level.INFO, "No worker pod for config map creation available...");
        		return this;
        	}
        	// create config map        	
        	controller.createConfigMap(cluster, workerPods.get(0));
        	
        	return WAIT_FOR_WORKERS_RUNNING;
        }
    },
    /**
     * WAIT_FOR_WORKERS_RUNNING
     * Wait for all workers to run
     */
    WAIT_FOR_WORKERS_RUNNING("WAIT_FOR_WORKERS_RUNNING") {
		@Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	// check for all nodes
        	SparkClusterState scs = super.reconcile(controller, cluster, cluster.getSpec().getMaster(), cluster.getSpec().getWorker());
        	if(scs != this) {
        		return scs;
        	}
        	
        	List<Pod> workerPods = controller.getPodsByNode(cluster, cluster.getSpec().getWorker());
        	super.reconcile(controller, cluster, cluster.getSpec().getMaster());

        	// wait for running
        	if(super.allPodsHaveStatus(workerPods, PodStatus.RUNNING)) {
        		return RECONCILE;
        	}
        	
        	SparkNode worker = cluster.getSpec().getWorker();
        	super.log(Level.INFO, String.format("%s [%d / %d]", worker.getPodTypeName(), workerPods.size(), worker.getInstances()));
        	
        	return this;
		}
    },
    /**
     * RECONCILE:
     * Watch the cluster state and act if spec != state
     */
    RECONCILE("RECONCILE") {
		@Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
			// scale and keep alive
			return super.reconcile(controller, cluster, cluster.getSpec().getMaster(), cluster.getSpec().getWorker());
		}
    };
    
	public abstract SparkClusterState process(SparkClusterController controller, SparkCluster cluster);
	
	public static final Logger logger = Logger.getLogger(SparkClusterState.class.getName());
	
	private String state;
	
    SparkClusterState(String state) {
		this.state = state;
	}
    
    public String toString() {
    	return state;
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
    private SparkClusterState reconcile(SparkClusterController controller, 
    									SparkCluster cluster, 
    									SparkNode... nodes) {
    	for(SparkNode node : nodes) {
	        List<Pod> pods = controller.getPodsByNode(cluster, node);
	        // create pods by changing state
	        if(getPodSpecToClusterDifference(node, pods) > 0) {
	        	// create masters
	        	if(node.getPodTypeName().equals(SparkNodeMaster.POD_TYPE))
	        		return CREATE_SPARK_MASTER;
	        	// create workers
	        	else if(node.getPodTypeName().equals(SparkNodeWorker.POD_TYPE))
	        		return CREATE_SPARK_WORKER;
	        }
	        // delete pods
	        if(getPodSpecToClusterDifference(node, pods) < 0) {
	        	List<Pod> deletedPods = controller.deletePods(pods, cluster, node);
	        	
	           	if(deletedPods.size() > 0) {
	        		List<String> output = new ArrayList<String>();
	        		deletedPods.forEach((n) -> output.add(n.getMetadata().getName()));
	        		log(Level.INFO, "Deleted pods: " + output);
	        	}
	        }
    	}
	   	return this;
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
    
    private void log(Level level, String message) {
    	logger.log(level, "STATE [" + state.toString() + "] - " + message);
    }
    
}
