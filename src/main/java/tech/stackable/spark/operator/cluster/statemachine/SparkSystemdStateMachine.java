package tech.stackable.spark.operator.cluster.statemachine;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import io.fabric8.kubernetes.api.model.Pod;
import tech.stackable.spark.operator.cluster.SparkCluster;
import tech.stackable.spark.operator.cluster.SparkClusterController;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusCommand;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusSystemd;
import tech.stackable.spark.operator.cluster.statemachine.SparkSystemdStateMachine.SystemdEvent;
import tech.stackable.spark.operator.common.state.SparkSystemdCommand;
import tech.stackable.spark.operator.common.state.SparkSystemdCommandState;

public class SparkSystemdStateMachine implements SparkStateMachine<SparkCluster, SystemdEvent> {

  private static final Logger LOGGER = Logger.getLogger(SparkSystemdStateMachine.class.getName());

  private SystemdState state;
  private final SparkClusterController controller;

  public SparkSystemdStateMachine(SparkClusterController controller) {
    state = SystemdState.SYSTEMD_READY;
    this.controller = controller;
  }

  @Override
  public boolean process(SparkCluster crd) {
    SystemdEvent event = getEvent(crd);
    if (event != SystemdEvent.READY) {
      transition(crd, event);
      return true;
    }
    return false;
  }

  @Override
  public SystemdEvent getEvent(SparkCluster crd) {
    // no status -> nothing to do
    if (crd.getStatus() == null || crd.getStatus().getSystemd() == null) {
      return SystemdEvent.READY;
    }

    SparkClusterStatusSystemd systemdStatus = crd.getStatus().getSystemd();

    // (no running command or running command is finished or failed) AND (staged command available)
    SystemdEvent event = SystemdEvent.READY;
    if ((systemdStatus.getRunningCommand() == null
      || systemdStatus.getRunningCommand().getStatus().equals(SparkSystemdCommandState.FAILED.toString())
      || systemdStatus.getRunningCommand().getStatus().equals(SparkSystemdCommandState.FINISHED.toString()))
      && systemdStatus.getStagedCommands().size() > 0) {
      event = SystemdEvent.START_COMMAND;
    }
    // running command == started
    else if (systemdStatus.getRunningCommand() != null
      && systemdStatus.getRunningCommand().getStatus().equals(SparkSystemdCommandState.STARTED.toString())) {
      SparkSystemdCommand systemdCommand = SparkSystemdCommand.getSystemdCommand(systemdStatus.getRunningCommand().getCommand());
      // event == update AND image not updated
      if (systemdCommand == SparkSystemdCommand.UPDATE
        && crd.getStatus().getImage().getName().equals(crd.getSpec().getImage())) {
        event = SystemdEvent.IMAGE_NOT_UPDATED;
      }
      // STOP
      else if (systemdCommand == SparkSystemdCommand.STOP) {
        event = SystemdEvent.STOP_COMMAND;
      }
      // move on for UPDATE and RESTART
      else {
        // TODO: check if jobs are finished
        event = SystemdEvent.JOBS_FINISHED;
      }
    }
    // command == running
    else if (systemdStatus.getRunningCommand() != null
      && systemdStatus.getRunningCommand().getStatus().equals(SparkSystemdCommandState.RUNNING.toString())) {
      List<Pod> pods = controller.getPodsByNode(crd, crd.getSpec().getMaster(), crd.getSpec().getWorker());
      // pods deleted?
      if (pods.isEmpty()) {
        event = SystemdEvent.PODS_DELETED;
      }
    }

    SystemdState old = state;
    state = state.nextState(event, state);
    // no changes if not initial
    if (old == state) {
      event = SystemdEvent.READY;
    }
    // else log
    else {
      LOGGER.debug(String.format("[%s] - systemd event: %s", state.name(), event.name()));
    }

    return event;
  }

  @Override
  public void transition(SparkCluster crd, SystemdEvent event) {
    switch (state) {
      case SYSTEMD_READY:
        break;
      case SYSTEMD_START_COMMAND:
        if (crd.getStatus().getSystemd().getStagedCommands().isEmpty()) {
          break;
        }

        String stagedCommand = crd.getStatus().getSystemd().getStagedCommands().remove(0);
        // set staged to running
        crd.getStatus().getSystemd().setRunningCommand(
          new SparkClusterStatusCommand.Builder()
            .withCommand(stagedCommand)
            .withStartedAt(String.valueOf(System.currentTimeMillis()))
            .withStatus(SparkSystemdCommandState.STARTED.toString())
            .build()
        );
        controller.getCrdClient().updateStatus(crd);
        break;
      case SYSTEMD_IMAGE_NOT_UPDATED:
        // no image updated
        if (crd.getStatus().getImage().getName().equals(crd.getSpec().getImage())) {
          // set status failed
          crd.getStatus().getSystemd().getRunningCommand().setStatus(SparkSystemdCommandState.FAILED.name());
          crd.getStatus().getSystemd().getRunningCommand().setReason("No updated image available!");
          controller.getCrdClient().updateStatus(crd);
          LOGGER.warn(String.format("[%s] - no updated image available: command %s",
            state, SparkSystemdCommandState.FAILED.name()));
        }
        break;
      case SYSTEMD_JOBS_FINISHED:
        // TODO: check if all spark jobs are finished
        if (true /* all spark jobs finished */) {
          // set staged to running
          crd.getStatus().getSystemd().getRunningCommand().setStatus(SparkSystemdCommandState.RUNNING.name());
          controller.getCrdClient().updateStatus(crd);
          // delete all pods
          List<Pod> deletedPods = controller.deletePods(crd, crd.getSpec().getMaster(), crd.getSpec().getWorker());

          LOGGER.debug(String.format("[%s] - deleted %d pod(s): %s",
            state, deletedPods.size(), SparkClusterController.metadataListToDebug(deletedPods)));
        }
        break;
      case SYSTEMD_PODS_DELETED:
        // set status running command to finished
        SparkClusterStatusCommand runningCommand = crd.getStatus().getSystemd().getRunningCommand();
        runningCommand.setStatus(SparkSystemdCommandState.FINISHED.toString());
        // set running command finished timestamp
        runningCommand.setFinishedAt(String.valueOf(System.currentTimeMillis()));
        crd.getStatus().getSystemd().setRunningCommand(runningCommand);

        controller.getCrdClient().updateStatus(crd);
        // TODO: reset state in cluster?
        break;
      case SYSTEMD_STOPPED:
        // wait for START
        break;
    }
  }

  /**
   * Systemd State Machine:
   *                        |
   *                        v
   * +<---------------- SYSTEMD_READY <-----------------+
   * | image not              | start command			      |
   * | updated                v						        ready |
   * |	          +<--- SYSTEMD_START_COMMAND <---------+
   * |            |           |	not updated			        ^
   * v            v           v	          start command	|
   * +----------->+---> SYSTEMD_IMAGE_NOT_UPDATED ----->+
   * | jobs       | jobs                                ^
   * v finished		v finished 	           					 			|
   * +----------->+---> SYSTEMD_JOBS_FINISHED			      |
   * | pods				| pods      | pods deleted			      |
   * v deleted		v deleted   v				       			ready	|
   * +----------->+---> SYSTEMD_PODS_DELETED ---------->+
   * |				 				        ^                         ^
   * v stop				    	  	  |                   ready |
   * +----------------> SYSTEMD_STOPPED --------------->+
   */
  public enum SystemdState {
    // states with their accepted transitions
    SYSTEMD_READY(
      SystemdEvent.START_COMMAND,
      SystemdEvent.STOP_COMMAND,
      SystemdEvent.JOBS_FINISHED,
      SystemdEvent.PODS_DELETED,
      SystemdEvent.IMAGE_NOT_UPDATED
    ),
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
    private static final Map<SystemdEvent, SystemdState> TRANSITIONS = new EnumMap<>(SystemdEvent.class);

    SystemdState(SystemdEvent... in) {
      events = Arrays.asList(in);
    }

    private SystemdState nextState(SystemdEvent event, SystemdState current) {
      SystemdState newState = current;
      if (events.contains(event)) {
        newState = TRANSITIONS.getOrDefault(event, current);
      }
      LOGGER.trace(String.format("[%s ==> %s]", current.name(), newState.name()));
      return newState;
    }

    // transitions (event,newState)
    static {
      TRANSITIONS.put(SystemdEvent.START_COMMAND, SYSTEMD_START_COMMAND);
      TRANSITIONS.put(SystemdEvent.IMAGE_NOT_UPDATED, SYSTEMD_IMAGE_NOT_UPDATED);
      TRANSITIONS.put(SystemdEvent.JOBS_FINISHED, SYSTEMD_JOBS_FINISHED);
      TRANSITIONS.put(SystemdEvent.PODS_DELETED, SYSTEMD_PODS_DELETED);
      TRANSITIONS.put(SystemdEvent.STOP_COMMAND, SYSTEMD_STOPPED);
      TRANSITIONS.put(SystemdEvent.READY, SYSTEMD_READY);
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
    STOP_COMMAND
  }

  public SystemdState getState() {
    return state;
  }

}
