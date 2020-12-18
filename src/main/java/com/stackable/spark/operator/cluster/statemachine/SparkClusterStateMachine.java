package com.stackable.spark.operator.cluster.statemachine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.SparkClusterController;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatus;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatusImage;
import com.stackable.spark.operator.cluster.crd.SparkNode;
import com.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine.ClusterEvent;
import com.stackable.spark.operator.common.state.PodState;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClientException;

public class SparkClusterStateMachine implements SparkStateMachine<SparkCluster, ClusterEvent> {
    private static final Logger logger = Logger.getLogger(SparkClusterStateMachine.class.getName());
    
	private ClusterState state;
	private SparkClusterController controller;
	
	public SparkClusterStateMachine(SparkClusterController controller) {
		this.state = ClusterState.INITIAL;
		this.controller = controller;
	}
	
	@Override
	public boolean process(SparkCluster cluster) {
		ClusterEvent event = getEvent(cluster);
		if(event != ClusterEvent.WAIT) {
			transition(cluster, event);
			return true;
		}
		return false;
	}

	/**
	 * Reconcile the spark cluster. Compare current with desired state and adapt via events
	 * @param controller - current spark cluster controller
	 * @param cluster - current spark cluster 
	 * @param nodes - master/worker to reconcile
	 * @return ClusterEvent:
	 * CREATE_MASTER if #masters < spec;
	 * WAIT_HOST_NAME if masters created but not running
	 * WAIT_MASTER_RUNNING if host names received and config map written
	 * CREATE_WORKER if #workers < spec;
	 * WAIT_WORKER_RUNNING if workers created but not running yet
	 * READY if cluster is in desired state
	 */
	@Override
	public ClusterEvent getEvent(SparkCluster cluster) {
    	SparkNode master = cluster.getSpec().getMaster();
    	SparkNode worker = cluster.getSpec().getWorker();
    	List<Pod> masterPods = controller.getPodsByNode(cluster, master);
    	List<Pod> workerPods = controller.getPodsByNode(cluster, worker);
    	
    	ClusterEvent event = ClusterEvent.READY;
  
        // delete excess master pods 
        // TODO: leader?
        if(controller.getPodSpecToClusterDifference(master, masterPods) < 0) {
        	controller.deletePods(masterPods, cluster, master);
        }
        // delete excess worker pods
        if(controller.getPodSpecToClusterDifference(worker, workerPods) < 0) {
        	controller.deletePods(workerPods, cluster, worker);
        }
        
		// masters missing
        if(controller.getPodSpecToClusterDifference(master, masterPods) > 0) {
        	event = ClusterEvent.CREATE_MASTER;
        }
        else if(controller.getHostNames(masterPods).size() == 0) {
			// got host name?
	    	// TODO who is leader?
        	event = ClusterEvent.WAIT;
        }
        // config map not written yet
        else if(controller.getHostNames(masterPods).size() != 0
        		&& controller.getConfigMap(cluster, master) == null) {
	    	// TODO who is leader?
        	event = ClusterEvent.WAIT_HOST_NAME;
        }
		// not all masters running
        else if(!controller.allPodsHaveStatus(masterPods, PodState.RUNNING)) {
    		event = ClusterEvent.WAIT_MASTER_RUNNING;
    	}
        // masters running - workers missing 
        else if(controller.getPodSpecToClusterDifference(worker, workerPods) > 0) {
        	event = ClusterEvent.CREATE_WORKER;
        }
		// all masters running - not all workers running
		else if(!controller.allPodsHaveStatus(workerPods, PodState.RUNNING)) {
			event = ClusterEvent.WAIT_WORKER_RUNNING;
       	}
		else {
			event = ClusterEvent.READY;
		}
        
        // only switch if not INITIAL
        if(state != ClusterState.INITIAL) {
        	ClusterState old = state;
        	state = state.nextState(event, state);
        	// no changes
        	if(old == state) {
        		event = ClusterEvent.WAIT;
        	}
        	// else log
        	else {
        		logger.debug(String.format("[%s] - %s [%d / %d] | %s [%d / %d]", 
        				state.name(),
        				master.getPodTypeName(), masterPods.size(), master.getInstances(),
        				worker.getPodTypeName(), workerPods.size(), worker.getInstances()
        		));
        	}
        }
    	return event;
	}

	/**
	 * Handle events and execute given states
	 * @param cluster - spark cluster
	 * @param event - given event
	 */
	@Override
	public void transition(SparkCluster cluster, ClusterEvent event) {
		try {
			switch(state) {
			case INITIAL: {
				// check for available status or create new one
				SparkClusterStatus status = cluster.getStatus() != null ? cluster.getStatus() : new SparkClusterStatus();
				// add spark image (deployedImage) to status for systemd update			
				status.setImage(
					new SparkClusterStatusImage(cluster.getSpec().getImage(), String.valueOf(System.currentTimeMillis()))
				);
				cluster.setStatus(status);
				// update status
				controller.getCrdClient().updateStatus(cluster);
				// TODO: hack
				state = ClusterState.READY;
				break;
			}
			case CREATE_MASTER: {
				SparkNode master = cluster.getSpec().getMaster();
				List<Pod> masterPods = controller.getPodsByNode(cluster, master);
				// delete old configmap
				controller.deleteConfigMap(cluster, master);
				// create master instances if required
	        	controller.createPods(masterPods, cluster, master);
				break;
			}
			case WAIT_HOST_NAME: {
				SparkNode master = cluster.getSpec().getMaster();
				List<Pod> masterPods = controller.getPodsByNode(cluster, master);
				List<String> masterHostNames = controller.getHostNames(masterPods);
				if(masterHostNames.size() != 0) {
					// TODO: actual master?
					logger.debug(String.format("[%s] - got host name: %s", state.name(), masterHostNames.get(0)));
					// create master config map when nodename received
					controller.createConfigMap(cluster, master);
				}
				break;
			}
			case WAIT_MASTER_RUNNING: {
				break;
			}
			case CREATE_WORKER: {
				SparkNode master = cluster.getSpec().getMaster();
				List<Pod> masterPods = controller.getPodsByNode(cluster, master);
				SparkNode worker = cluster.getSpec().getWorker();
				List<Pod> workerPods = controller.getPodsByNode(cluster, worker);
				
	        	// check host name
	        	List<String> masterNodeNames = controller.getHostNames(masterPods);
	        	
	        	if( masterNodeNames.size() == 0) {
	        		logger.warn(String.format(""));
	        		break;
	        	}
	        	// adapt command in workers
	        	controller.adaptWorkerCommand(cluster, masterNodeNames);
	        	// create config map        	
	        	controller.createConfigMap(cluster, worker);
	        	// spin up workers
	        	workerPods = controller.createPods(workerPods, cluster, worker);
	        	break;
			}
			case WAIT_WORKER_RUNNING: {
				break;
			}
			case READY: {
				break;
			}}
		}
		catch(KubernetesClientException ex) {
			logger.warn("Received outdated object: " + ex.getMessage());
		}
	}
	
	/**
	 * Cluster state machine:
	 * 					|
	 * 					v
	 * 	+<--+<----- CLUSTER_INITIAL
	 * 	|	|create		| create master	
	 * 	|	|worker		v	
	 *  |	|		CREATE_MASTER <-----------------+
	 *	|	|	  		| wait host name			^
	 *	|	|			|							|
	 *  |	|			v			create master	|
	 *  |	|		WAIT_HOST_NAME ---------------->+
	 *  |	|			| master running			^
	 *  |	|			|							|
	 *  |	|			v			create master	|
	 *  |	|		WAIT_MASTER_RUNNING ----------->+
	 *  |	|			| create worker				|
	 *  |	|			v							|
	 *  |	+----->	CREATE_WORKER ----------------->+
	 *  |	^			| worker running			^
	 *  |	|create		|							|
	 *  |	|worker		v			create master	|
	 *  |	+<-----	WAIT_WORKER_RUNNING ----------->+
	 *  |	|create		| ready						|
	 *  |	|worker		v			create master	|
	 *  |	+------	CLUSTER_READY ----------------->+
	 *  |				^
	 *  +---------------+
	 */
    public enum ClusterState {
    	// states with their accepted transitions
    	/**
    	 * Set cluster timestamp in status
    	 */
    	INITIAL(
    		ClusterEvent.CREATE_MASTER,	
        	ClusterEvent.CREATE_WORKER,
        	ClusterEvent.READY
    	),
    	/**
    	 * Spawn master pods given by specification
    	 */
        CREATE_MASTER(
        	ClusterEvent.WAIT_HOST_NAME
        ),
        /**
         * after master is created, wait for agent to set the dedicated host name and use for workers 
         * ==> create config map
         */
        WAIT_HOST_NAME(
        	ClusterEvent.CREATE_MASTER,
        	ClusterEvent.WAIT_MASTER_RUNNING
        ),
        /**
         * wait for all masters running
         */
        WAIT_MASTER_RUNNING(
            ClusterEvent.CREATE_MASTER,
            ClusterEvent.CREATE_WORKER,
            ClusterEvent.READY
        ),
        /**
         * Spawn worker pods given by specification
         * ==> create config map
         */
        CREATE_WORKER(
        	ClusterEvent.CREATE_MASTER,
        	ClusterEvent.WAIT_WORKER_RUNNING
        ),
        /**
         * Wait for all running
         */
        WAIT_WORKER_RUNNING(
			ClusterEvent.CREATE_MASTER,
			ClusterEvent.CREATE_WORKER,
			ClusterEvent.READY
        ),
    	/**
    	 * Watch the cluster state and act if spec != state
    	 */
        READY(
        	ClusterEvent.CREATE_MASTER,
        	ClusterEvent.WAIT_HOST_NAME,
        	ClusterEvent.WAIT_MASTER_RUNNING,
        	ClusterEvent.CREATE_WORKER,
        	ClusterEvent.WAIT_WORKER_RUNNING
        );

        private final List<ClusterEvent> events;
        private final static Map<ClusterEvent, ClusterState> map = new HashMap<>();
    	
        ClusterState(ClusterEvent... in) {
            this.events = Arrays.asList(in);
        }
 
        public ClusterState nextState(ClusterEvent event, ClusterState current) {
        	ClusterState newState = current;
            if (events.contains(event)) {
                newState = map.getOrDefault(event, current);
            }
            logger.trace(String.format("[%s ==> %s]", current.name(), newState.name()));
            return newState;
        }
        // transitions (event,newState)
        static {
            map.put(ClusterEvent.CREATE_MASTER, ClusterState.CREATE_MASTER);
            map.put(ClusterEvent.WAIT_HOST_NAME, ClusterState.WAIT_HOST_NAME);
            map.put(ClusterEvent.WAIT_MASTER_RUNNING, ClusterState.WAIT_MASTER_RUNNING);
            map.put(ClusterEvent.CREATE_WORKER, ClusterState.CREATE_WORKER);
            map.put(ClusterEvent.WAIT_WORKER_RUNNING, ClusterState.WAIT_WORKER_RUNNING);
            map.put(ClusterEvent.READY, ClusterState.READY);
        }
    }
	
	/**
	 * cluster state transition events
	 */
	public enum ClusterEvent {
		/**
		 * event for creating master
		 */
		CREATE_MASTER,
		/*
		 * event to wait for hostname
		 */
		WAIT_HOST_NAME,
		/**
		 * event for all masters running
		 */
		WAIT_MASTER_RUNNING,
		/**
		 * event to create all workers
		 */
		CREATE_WORKER,
		/**
		 * event for all workers running
		 */
		WAIT_WORKER_RUNNING,
		/**
		 * event to signal cluster is ready
		 */
		READY,
		/**
		 * Nothing to do
		 */
		WAIT
	}
	
    public ClusterState getState() {
    	return state;
    }

}
