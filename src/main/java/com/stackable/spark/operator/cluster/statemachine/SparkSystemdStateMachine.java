package com.stackable.spark.operator.cluster.statemachine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.SparkClusterController;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatusCommand;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatusSystemd;
import com.stackable.spark.operator.cluster.crd.SparkNode;
import com.stackable.spark.operator.cluster.statemachine.SparkSystemdStateMachine.SystemdEvent;
import com.stackable.spark.operator.common.state.SparkSystemdCommandState;

import io.fabric8.kubernetes.api.model.Pod;

public class SparkSystemdStateMachine implements SparkStateMachine<SparkCluster, SystemdEvent> {
    private static final Logger logger = Logger.getLogger(SparkSystemdStateMachine.class.getName());
    
	private SystemdState state;
	private SparkClusterController controller;
	
	public SparkSystemdStateMachine(SparkClusterController controller) {
		this.state = SystemdState.SYSTEMD_READY;
		this.controller = controller;
	}
	
	@Override
	public boolean process(SparkCluster cluster) {
		SystemdEvent event = getEvent(cluster);
		if( event == SystemdEvent.NO_EVENT) {
			return false;
		}
		transition(cluster, event);
		return true;
	}
	
	@Override
	public SystemdEvent getEvent(SparkCluster cluster) {
		// TODO: improve event finding depending on status and command
		SystemdEvent event = SystemdEvent.NO_EVENT;
		// check if status available: no status nothing to do
		// NO_EVENT
		if(cluster.getStatus() == null || cluster.getStatus().getSystemd() == null) { 
			return event;
		}
		
		SparkClusterStatusSystemd status = cluster.getStatus().getSystemd();
		// check if command is running and not finished
		if(	status.getRunningCommand() != null && 
			status.getRunningCommand().getCommand() != null &&
			!status.getRunningCommand().getStatus().equals(SparkSystemdCommandState.FINISHED.toString())) {
			// TODO: improve
			event = SystemdEvent.getSystemdEvent(status.getRunningCommand().getCommand());
		}
		// check staged command with no running command
		// systemd: START, STOP, UPDATE, RESTART,
		if(status.getStagedCommands().size() != 0) {
			event = SystemdEvent.getSystemdEvent(status.getStagedCommands().get(0));
		}
		
		return event;
	}
	

	@Override
	public void transition(SparkCluster cluster, SystemdEvent event) {
		switch(state) {
		case SYSTEMD_READY: {
			logger.debug(String.format("[%s] - systemd event: %s", state.name(), event.name()));
			
			String stagedCommand = cluster.getStatus().getSystemd().getStagedCommands().remove(0);
			// set staged to running
			cluster.getStatus().getSystemd().setRunningCommand(
				new SparkClusterStatusCommand.Builder()
					.withCommand(stagedCommand)
					.withStartedAt(String.valueOf(System.currentTimeMillis()))
					.withStatus(SparkSystemdCommandState.STARTED.toString())
					.build()
				);
			// update status
			controller.getCrdClient().updateStatus(cluster);
			
			state = state.nextState(event, state);
			break;
		}
		case SYSTEMD_IMAGE_UPDATED: {
			if(!cluster.getStatus().getImage().getName().equals(cluster.getSpec().getImage())) {
				// send image updated event
				event = SystemdEvent.IMAGE_UPDATED;
			}
			else { 
				logger.warn("systemd update called but no new image specified: "
							+ "old(" + cluster.getStatus().getImage().getName() + ") - new(" + cluster.getSpec().getImage() + ")");

			}
			logger.debug(String.format("[%s] - event: %s", state.name(), event.name()));
			state = state.nextState(event, state);
			//TODO: break;
		}
		case SYSTEMD_JOBS_FINISHED: {
			// TODO: check if all spark jobs are finished
			if(true /* all spark jobs finished */) {
				// set staged to running
				cluster.getStatus().getSystemd().getRunningCommand().setStatus(SparkSystemdCommandState.RUNNING.name());
				// update status
				controller.getCrdClient().updateStatus(cluster);
				// delete all pods
				controller.deletePods(cluster, state.name());
				// send pods deleted event
				event = SystemdEvent.JOBS_FINISHED;
				
				logger.debug(String.format("[%s] - event: %s", state.name(), event.name()));
				state = state.nextState(event, state);
			}
			break;
		}
		case SYSTEMD_PODS_DELETED: {
			List<Pod> pods = controller.getPodsByNode(cluster, (SparkNode[])null);
			// still pods available?
			if(pods.size() > 0) {
				// stop and wait
				break;
			}
			// set status running command to finished
			SparkClusterStatusCommand runningCommand = cluster.getStatus().getSystemd().getRunningCommand();
			runningCommand.setStatus(SparkSystemdCommandState.FINISHED.toString());
			// set running command finished timestamp 
			runningCommand.setFinishedAt(String.valueOf(System.currentTimeMillis()));
			cluster.getStatus().getSystemd().setRunningCommand(runningCommand);
			// update crd status
			controller.getCrdClient().updateStatus(cluster);
		
			// check for stopped command -> wait in stopped
			if(runningCommand.getCommand().toUpperCase().equals(SystemdEvent.STOP.name())) {
				event = SystemdEvent.STOP;
			} 
			// reset and go to ready
			else {
				event = SystemdEvent.PODS_DELETED;
			}
			
			logger.debug(String.format("[%s] - event: %s", state.name(), event.name()));
			state = state.nextState(event, state);			
			// TODO: reset state in cluster?
			break;
		}
		case SYSTEMD_STOPPED: {
			// wait for START, UPDATE, RESTART event
			logger.debug(String.format("[%s] - event: %s", state.name(), event.name()));
			state = state.nextState(event, state);
			break;
		}}
	}
	
	/**
	 * Systemd State Machine:
	 *					|
	 * 					v						
	 * 			+---SYSTEMD_READY <-------------+
	 * 			|		| update				|
	 * restart	|		v						|
	 * 			|	SYSTEMD_IMAGE_UPDATED		|
	 * 	stop	| 		| image_updated			|
	 * 			|		v						|
	 * 			+-> SYSTEMD_JOBS_FINISHED		|
	 * 					| jobs_finished			|
	 * 					v				implicit|
	 * 				SYSTEMD_PODS_DELETED ------>+
	 *	 				| stop					^
	 * 					v				start	|
	 * 				SYSTEMD_STOPPED ------------+
	 */
    public enum SystemdState {
    	// states with their accepted transitions
    	SYSTEMD_READY(
			SystemdEvent.RESTART, 
			SystemdEvent.UPDATE, 
			SystemdEvent.STOP
    	),
    	SYSTEMD_IMAGE_UPDATED(
    		SystemdEvent.IMAGE_UPDATED
    	),
    	SYSTEMD_JOBS_FINISHED(
    		SystemdEvent.JOBS_FINISHED
    	),
    	SYSTEMD_PODS_DELETED(
    		SystemdEvent.PODS_DELETED
    	),
    	SYSTEMD_STOPPED(
    		SystemdEvent.RESTART, 
    		SystemdEvent.UPDATE, 
    		SystemdEvent.START
    	);

        private final List<SystemdEvent> events;
        private final static Map<SystemdEvent, SystemdState> map = new HashMap<>();
    	
        SystemdState(SystemdEvent... in) {
            this.events = Arrays.asList(in);
        }
 
        public SystemdState nextState(SystemdEvent event, SystemdState current) {
            if (events.contains(event)) {
                return map.getOrDefault(event, current);
            }
            return current;
        }
        // transitions (event,newState)
        static {
        	// SYSTEMD_READY
            map.put(SystemdEvent.RESTART, SystemdState.SYSTEMD_JOBS_FINISHED);
            map.put(SystemdEvent.UPDATE, SystemdState.SYSTEMD_IMAGE_UPDATED);
            map.put(SystemdEvent.STOP, SystemdState.SYSTEMD_JOBS_FINISHED);
            map.put(SystemdEvent.IMAGE_UPDATED, SystemdState.SYSTEMD_JOBS_FINISHED);
            map.put(SystemdEvent.JOBS_FINISHED, SystemdState.SYSTEMD_PODS_DELETED);
            map.put(SystemdEvent.PODS_DELETED, SystemdState.SYSTEMD_READY);
            map.put(SystemdEvent.START, SystemdState.SYSTEMD_READY);
        }
    }
	
	/**
	 * Systemd state transition events
	 */
	public enum SystemdEvent {
		/**
		 * nothing to be done
		 */
		NO_EVENT,
		/**
		 * Systemd start
		 */
		START,
		/**
		 * Systemd stop 
		 */
		STOP,
		/**
		 * Systemd update
		 */
		UPDATE,
		/**
		 * Systemd restart
		 */
		RESTART,
		/**
		 * Event if spark image in crd is updated
		 */
		IMAGE_UPDATED,
		/**
		 * Event if all spark jobs are finished
		 */
		JOBS_FINISHED,
		/**
		 * Event if all available cluster pods are deleted
		 */
		PODS_DELETED;
		
		/**
		 * Retrieve first staged status command
		 * @param commands - list of staged commands
		 * @return SparkSystemAction in staged command (e.g. RESTART, UPDATE)
		 */
		public static SystemdEvent getSystemdEvent(String command) {
			if(command != null) {
				SystemdEvent[] events = new SystemdEvent[] {START, STOP, UPDATE, RESTART};
				
				for (SystemdEvent event : events) { 
					if(event.name().equals(command.toUpperCase()))
						return event;
				}
			}
			return SystemdEvent.NO_EVENT;
		}
	}
	
    public SystemdState getState() {
    	return state;
    }

}
