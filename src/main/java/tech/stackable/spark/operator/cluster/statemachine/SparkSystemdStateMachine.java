package tech.stackable.spark.operator.cluster.statemachine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import io.fabric8.kubernetes.api.model.Pod;
import tech.stackable.spark.operator.cluster.SparkCluster;
import tech.stackable.spark.operator.cluster.SparkClusterController;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusCommand;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusSystemd;
import tech.stackable.spark.operator.cluster.crd.SparkNode;
import tech.stackable.spark.operator.cluster.statemachine.SparkSystemdStateMachine.SystemdEvent;
import tech.stackable.spark.operator.common.state.SparkSystemdCommand;
import tech.stackable.spark.operator.common.state.SparkSystemdCommandState;

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
		if(event != SystemdEvent.READY) {
			transition(cluster, event);
			return true;
		}
		return false;
	}
	
	@Override
	public SystemdEvent getEvent(SparkCluster cluster) {
		SystemdEvent event = SystemdEvent.READY;
		// no status -> nothing to do
		if(cluster.getStatus() == null || cluster.getStatus().getSystemd() == null) { 
			return SystemdEvent.READY;
		}
		
		SparkClusterStatusSystemd systemdStatus = cluster.getStatus().getSystemd();
		
		// (no running command or running command is finished or failed) AND (staged command available)
		if((systemdStatus.getRunningCommand() == null
			|| systemdStatus.getRunningCommand().getStatus().equals(SparkSystemdCommandState.FAILED.toString())
			|| systemdStatus.getRunningCommand().getStatus().equals(SparkSystemdCommandState.FINISHED.toString()))
			&& systemdStatus.getStagedCommands().size() > 0) {
			event = SystemdEvent.START_COMMAND;
		}
		// running command == started
		else if(systemdStatus.getRunningCommand() != null
				&& systemdStatus.getRunningCommand().getStatus().equals(SparkSystemdCommandState.STARTED.toString())) {
			SparkSystemdCommand systemdCommand = SparkSystemdCommand.getSystemdCommand(systemdStatus.getRunningCommand().getCommand());
			// event == update AND image not updated
			if(systemdCommand == SparkSystemdCommand.UPDATE
				&& cluster.getStatus().getImage().getName().equals(cluster.getSpec().getImage())) {
				event = SystemdEvent.IMAGE_NOT_UPDATED;
			}
			// STOP 
			else if( systemdCommand == SparkSystemdCommand.STOP) {
				event = SystemdEvent.STOP_COMMAND;
			}
			// move on for UPDATE and RESTART
			else {
				// TODO: check if jobs are finished
				event = SystemdEvent.JOBS_FINISHED;
			}
		}
		// command == running
		else if(systemdStatus.getRunningCommand() != null
				&& systemdStatus.getRunningCommand().getStatus().equals(SparkSystemdCommandState.RUNNING.toString())) {
			List<Pod> pods = controller.getPodsByNode(cluster, (SparkNode[])null);
			// pods deleted?
			if(pods.size() == 0) {
				event = SystemdEvent.PODS_DELETED;
			}
		}
		
       	SystemdState old = state;
       	state = state.nextState(event, state);
      	// no changes if not initial
       	if(old == state) {
       		event = SystemdEvent.READY;
       	}
       	// else log
       	else {
       		logger.debug(String.format("[%s] - systemd event: %s", state.name(), event.name()));
        }
		
		return event;
	}

	@Override
	public void transition(SparkCluster cluster, SystemdEvent event) {
		switch(state) {
		case SYSTEMD_READY: {
			break;
		}
		case SYSTEMD_START_COMMAND: {
			if(cluster.getStatus().getSystemd().getStagedCommands().size() == 0) {
				break;
			}
			
			String stagedCommand = cluster.getStatus().getSystemd().getStagedCommands().remove(0);
			// set staged to running
			cluster.getStatus().getSystemd().setRunningCommand(
				new SparkClusterStatusCommand.Builder()
					.withCommand(stagedCommand)
					.withStartedAt(String.valueOf(System.currentTimeMillis()))
					.withStatus(SparkSystemdCommandState.STARTED.toString())
					.build()
				);
			controller.getCrdClient().updateStatus(cluster);
			break;
		}
		case SYSTEMD_IMAGE_NOT_UPDATED: {
			// no image updated
			if(cluster.getStatus().getImage().getName().equals(cluster.getSpec().getImage())) {
				// set status failed
				cluster.getStatus().getSystemd().getRunningCommand().setStatus(SparkSystemdCommandState.FAILED.name());
				cluster.getStatus().getSystemd().getRunningCommand().setReason("No updated image available!");
				controller.getCrdClient().updateStatus(cluster);
				logger.warn(String.format("[%s] - no updated image available: command %s", 
							state, SparkSystemdCommandState.FAILED.name()));
			}
			break;
		}
		case SYSTEMD_JOBS_FINISHED: {
			// TODO: check if all spark jobs are finished
			if(true /* all spark jobs finished */) {
				// set staged to running
				cluster.getStatus().getSystemd().getRunningCommand().setStatus(SparkSystemdCommandState.RUNNING.name());
				controller.getCrdClient().updateStatus(cluster);
				// delete all pods
				List<Pod> deletedPods = controller.deletePods(cluster);
				
				logger.debug(String.format("[%s] - deleted %d pod(s): %s", 
						state, deletedPods.size(), controller.metadataListToDebug(deletedPods)));
			}
			break;
		}
		case SYSTEMD_PODS_DELETED: {
			// set status running command to finished
			SparkClusterStatusCommand runningCommand = cluster.getStatus().getSystemd().getRunningCommand();
			runningCommand.setStatus(SparkSystemdCommandState.FINISHED.toString());
			// set running command finished timestamp 
			runningCommand.setFinishedAt(String.valueOf(System.currentTimeMillis()));
			cluster.getStatus().getSystemd().setRunningCommand(runningCommand);
			
			controller.getCrdClient().updateStatus(cluster);
			// TODO: reset state in cluster?
			break;
		}
		case SYSTEMD_STOPPED: {
			// wait for START
			break;
		}}
	}
	
	/**
	 * Systemd State Machine:
	 * 							|
	 * 							v	
	 * 			+<-------SYSTEMD_READY <----------------+
	 * 			|				| start command			|
	 * 			| jobs finished	v						|
	 * 			|	+<--SYSTEMD_START_COMMAND <---------+
	 * 			|	|			| not updated			^
	 * 			|	|			v 						|
	 *  		|	|	SYSTEMD_IMAGE_NOT_UPDATED ----->+ ready, start command
	 * jobs		v	v 					 				^
	 * finished +-->+-> SYSTEMD_JOBS_FINISHED			|
	 * 			|				| pods deleted			|
	 * pods		v				v			ready		|
	 * deleted	+----->	SYSTEMD_PODS_DELETED ---------->+
	 *	 		|				 						^
	 * 			v							ready		|
	 * 	stop	+-----> SYSTEMD_STOPPED --------------->+
	 */
    public enum SystemdState {
    	SYSTEMD_READY(
    		SystemdEvent.START_COMMAND,
    		SystemdEvent.STOP_COMMAND,
        	SystemdEvent.JOBS_FINISHED,
        	SystemdEvent.PODS_DELETED,
        	SystemdEvent.IMAGE_NOT_UPDATED
    	),
    	// states with their accepted transitions
    	SYSTEMD_START_COMMAND(
    		SystemdEvent.JOBS_FINISHED,
    		SystemdEvent.PODS_DELETED,
    		SystemdEvent.IMAGE_NOT_UPDATED
    	),
    	SYSTEMD_IMAGE_NOT_UPDATED(
    		SystemdEvent.START_COMMAND,
    		SystemdEvent.READY
    	),
    	SYSTEMD_JOBS_FINISHED(
    		SystemdEvent.PODS_DELETED
    	),
    	SYSTEMD_PODS_DELETED(
    		SystemdEvent.READY
    	),
    	SYSTEMD_STOPPED(
    		SystemdEvent.READY
    	);

        private final List<SystemdEvent> events;
        private final static Map<SystemdEvent, SystemdState> map = new HashMap<>();
    	
        SystemdState(SystemdEvent... in) {
            this.events = Arrays.asList(in);
        }
 
        public SystemdState nextState(SystemdEvent event, SystemdState current) {
        	SystemdState newState = current;
            if (events.contains(event)) {
                newState = map.getOrDefault(event, current);
            }
            logger.trace(String.format("[%s ==> %s]", current.name(), newState.name()));
            return newState;
        }
        // transitions (event,newState)
        static {
        	map.put(SystemdEvent.START_COMMAND, SystemdState.SYSTEMD_START_COMMAND);
            map.put(SystemdEvent.IMAGE_NOT_UPDATED, SystemdState.SYSTEMD_IMAGE_NOT_UPDATED);
            map.put(SystemdEvent.JOBS_FINISHED, SystemdState.SYSTEMD_JOBS_FINISHED);
            map.put(SystemdEvent.PODS_DELETED, SystemdState.SYSTEMD_PODS_DELETED);
            map.put(SystemdEvent.STOP_COMMAND, SystemdState.SYSTEMD_STOPPED);
            map.put(SystemdEvent.READY, SystemdState.SYSTEMD_READY);
        }
    }
	
	/**
	 * Systemd state transition events
	 */
	public enum SystemdEvent {
		/**
		 * systemd ready event
		 */
		READY,
		/**
		 * event to start command
		 */
		START_COMMAND,
		/**
		 * event if spark image in crd is not updated
		 */
		IMAGE_NOT_UPDATED,
		/**
		 * event if all spark jobs are finished
		 */
		JOBS_FINISHED,
		/**
		 * event if all available cluster pods are deleted
		 */
		PODS_DELETED,
		/**
		 * event if stop command arrived
		 */
		STOP_COMMAND;
	}
	
    public SystemdState getState() {
    	return state;
    }

}
