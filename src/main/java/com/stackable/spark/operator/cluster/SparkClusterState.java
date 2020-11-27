package com.stackable.spark.operator.cluster;

import java.util.List;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.controller.SparkClusterController;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;

public enum SparkClusterState {
	/**
	 * INITIAL: wait for cluster description and create masters
	 */
    INITIAL("INITIAL") {
        @Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	logger.info("STATE ==> " + INITIAL.toString());

        	// reset hostMap
        	controller.getHostNameMap().clear();
        	
        	// create master instances
        	controller.reconcile(cluster, cluster.getSpec().getMaster());
        	
			// all master pods created?
			if(controller.getPodsByNode(cluster, cluster.getSpec().getMaster()).size() ==
					cluster.getSpec().getMaster().getInstances()) {
				return WAIT_FOR_MASTER_HOST_NAME; 
			}
        	
            // wait for pod creation and host name
            return INITIAL;
        }
    },
    
    /**
     * WAIT_FOR_MASTER_HOST: 
     * after master is created, wait for agent to set the dedicated hostname and use for workers 
     * ==> create config map
     */
    WAIT_FOR_MASTER_HOST_NAME("WAIT_FOR_MASTER_HOST_NAME") {
        @Override
        public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	logger.info("STATE ==> " + WAIT_FOR_MASTER_HOST_NAME.toString());
            
        	List<Pod> masterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
        	// any masters?
        	if(masterPods.size() == 0) {
        		return INITIAL;
        	}
        	// TODO: multiple master?
			// check for hostname
        	String nodeName = masterPods.get(0).getSpec().getNodeName();
        	if( nodeName == null || nodeName.isEmpty()) {
        		return this;
        	}
        	
    		logger.info("Received hostname: " + nodeName  + " for pod: " + masterPods.get(0).getMetadata().getName());
    		// save host name for workers
    		controller.addToHostMap(masterPods.get(0).getMetadata().getName(), nodeName);
    		// create configmap
    		controller.createConfigMap(cluster, masterPods.get(0));
    		
   			return WAIT_FOR_MASTER_READY;
        }

    },
    
    /**
     * Wait before the master is configured and running before spawning workers
     */
    WAIT_FOR_MASTER_READY("WAIT_FOR_MASTER_READY") {
        @Override
        public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	logger.info("STATE ==> " + WAIT_FOR_MASTER_READY.toString());
        	
        	List<Pod> masterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
        	// any masters?
        	if(masterPods.size() == 0) {
        		return INITIAL;
        	}
        	
    		boolean allRunning = true;
        	for(Pod master : masterPods) {
        		if(!master.getStatus().getPhase().equals("Running")) {
        			allRunning = false;
        		}
        	}
        	
        	if(allRunning == false) {
        		return this;
        	}
        	
            return CREATE_SPARK_WORKER;
        }

    },
    
    CREATE_SPARK_WORKER("CREATE_SPARK_WORKER") {
        @Override
        public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	logger.info("STATE ==> " + CREATE_SPARK_WORKER.toString());
        	
        	List<Pod> masterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
        	// any masters?
        	if(masterPods.size() == 0) {
        		return INITIAL;
        	}
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
        		logger.info("Set worker MASTER_URL to: " + masterUrl);
        	}
        	
        	// spin up workers
        	controller.reconcile(cluster, cluster.getSpec().getWorker() );
            return WAIT_FOR_WORKERS_READY;
        }
    },
    
    WAIT_FOR_WORKERS_READY("WAIT_FOR_WORKERS_READY") {
		@Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
			logger.info("STATE ==> " + WAIT_FOR_WORKERS_READY.toString());
			
        	List<Pod> workerPods = controller.getPodsByNode(cluster, cluster.getSpec().getWorker());
        	// any masters?
        	if(workerPods.size() == 0) {
        		return CREATE_SPARK_WORKER;
        	}

    		boolean allRunning = true;
        	for(Pod worker : workerPods) {
        		if(!worker.getStatus().getPhase().equals("Running")) {
        			allRunning = false;
        		}
        	}
        	
        	if(allRunning == false) {
        		return this;
        	}
			// all running continue
			return RECONCILE_MASTER;
		}
    },
    
    RECONCILE_MASTER("RECONCILE_MASTER") {
		@Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
			logger.info("STATE ==> " + RECONCILE_MASTER.toString());
			// scale and keep alive
			controller.reconcile(cluster, cluster.getSpec().getMaster());
			return RECONCILE_WORKER;
		}
    },
    
    RECONCILE_WORKER("RECONCILE_WORKER") {
		@Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
			logger.info("STATE ==> " + RECONCILE_WORKER.toString());
			// scale and keep alive
			return controller.reconcile(cluster, cluster.getSpec().getWorker());
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

}
