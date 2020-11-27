package com.stackable.spark.operator.cluster;

import java.util.List;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.controller.SparkClusterController;

import io.fabric8.kubernetes.api.model.Pod;

public enum SparkClusterState {
	/**
	 * INITIAL: wait for cluster description and create masters
	 */
    INITIAL("INITIAL") {
        @Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	logger.info("STATE -> " + INITIAL.toString());

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
     * -> create config map
     */
    WAIT_FOR_MASTER_HOST_NAME("WAIT_FOR_MASTER_HOST_NAME") {
        @Override
        public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	logger.info("STATE -> " + WAIT_FOR_MASTER_HOST_NAME.toString());
            
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
    			
    		// TODO: create config map
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
        	logger.info("STATE -> " + WAIT_FOR_MASTER_READY.toString());
        	
        	List<Pod> masterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
        	// any masters?
        	if(masterPods.size() == 0) {
        		return INITIAL;
        	}
        	
        	if(masterPods.get(0).getStatus().getPhase().equals("Running")) {
        		return CREATE_SPARK_WORKER;
        	}
        	
            return this;
        }

    },
    
    CREATE_SPARK_WORKER("CREATE_SPARK_WORKER") {
        @Override
        public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
        	logger.info("STATE -> " + CREATE_SPARK_WORKER.toString());
        	
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
        	// adapt command in workers
        	List<String> commands = cluster.getSpec().getWorker().getCommand();
        	// TODO: start-worker.sh? - adapt port
        	if(commands.size() == 1) {
        		commands.add("spark://" + masterHostName + ":7077");
        	}
        	
        	// spin up workers
        	controller.reconcile(cluster, cluster.getSpec().getWorker() );
            return WAIT_FOR_WORKERS_READY;
        }
    },
    
    WAIT_FOR_WORKERS_READY("WAIT_FOR_WORKERS_READY") {
		@Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
			logger.info("STATE -> " + RECONCILE_CLUSTER.toString());
			
        	List<Pod> workerPods = controller.getPodsByNode(cluster, cluster.getSpec().getWorker());
        	// any workers?
        	if(workerPods.size() == 0) {
        		return CREATE_SPARK_WORKER;
        	}
        	
        	if(workerPods.size() < cluster.getSpec().getWorker().getInstances()) {
        		return this;
        	}
        	
        	// create worker cm
    		controller.createConfigMap(cluster, workerPods.get(0));
			
			return RECONCILE_CLUSTER;
		}
    },
    
    RECONCILE_CLUSTER("RECONCILE_CLUSTER") {
		@Override
		public SparkClusterState process(SparkClusterController controller, SparkCluster cluster) {
			logger.info("STATE -> " + RECONCILE_CLUSTER.toString());
			controller.reconcile(cluster, cluster.getSpec().getMaster(), cluster.getSpec().getWorker());
			return RECONCILE_CLUSTER;
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
