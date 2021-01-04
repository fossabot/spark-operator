package tech.stackable.spark.operator.cluster.statemachine;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.SparkClusterController;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusCommand;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusManager;
import tech.stackable.spark.operator.cluster.statemachine.SparkManagerStateMachine.ManagerEvent;
import tech.stackable.spark.operator.common.state.SparkManagerCommand;
import tech.stackable.spark.operator.common.state.SparkManagerCommandState;

public class SparkManagerStateMachine implements SparkStateMachine<SparkCluster, ManagerEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkManagerStateMachine.class);

  private ManagerState state;
  private final SparkClusterController controller;

  public SparkManagerStateMachine(SparkClusterController controller) {
    state = ManagerState.MANAGER_READY;
    this.controller = controller;
  }

  @Override
  public boolean process(SparkCluster crd) {
    ManagerEvent event = getEvent(crd);
    if (event != ManagerEvent.READY) {
      transition(crd, event);
      return true;
    }
    return false;
  }

  @Override
  public ManagerEvent getEvent(SparkCluster crd) {
    // no status -> nothing to do
    if (crd.getStatus() == null || crd.getStatus().getManager() == null) {
      return ManagerEvent.READY;
    }

    SparkClusterStatusManager managerStatus = crd.getStatus().getManager();

    // (no running command or running command is finished or failed) AND (staged command available)
    ManagerEvent event = ManagerEvent.READY;
    if ((managerStatus.getRunningCommand() == null
      || managerStatus.getRunningCommand().getStatus().equals(SparkManagerCommandState.FAILED.toString())
      || managerStatus.getRunningCommand().getStatus().equals(SparkManagerCommandState.FINISHED.toString()))
      && managerStatus.getStagedCommands().size() > 0) {
      event = ManagerEvent.START_COMMAND;
    }
    // running command == started
    else if (managerStatus.getRunningCommand() != null
      && managerStatus.getRunningCommand().getStatus().equals(SparkManagerCommandState.STARTED.toString())) {
      SparkManagerCommand managerCommand = SparkManagerCommand.getSystemdCommand(managerStatus.getRunningCommand().getCommand());
      // event == update AND image not updated
      if (managerCommand == SparkManagerCommand.UPDATE
        && crd.getStatus().getImage().getName().equals(crd.getSpec().getImage())) {
        event = ManagerEvent.IMAGE_NOT_UPDATED;
      }
      // STOP
      else if (managerCommand == SparkManagerCommand.STOP) {
        event = ManagerEvent.STOP_COMMAND;
      }
      // move on for UPDATE and RESTART
      else {
        // TODO: check if jobs are finished
        event = ManagerEvent.JOBS_FINISHED;
      }
    }
    // command == running
    else if (managerStatus.getRunningCommand() != null
      && managerStatus.getRunningCommand().getStatus().equals(SparkManagerCommandState.RUNNING.toString())) {
      List<Pod> pods = controller.getPodsByNode(crd, crd.getSpec().getMaster(), crd.getSpec().getWorker());
      // pods deleted?
      if (pods.isEmpty()) {
        event = ManagerEvent.PODS_DELETED;
      }
    }

    ManagerState old = state;
    state = state.nextState(event, state);
    // no changes if not initial
    if (old == state) {
      event = ManagerEvent.READY;
    }
    // else log
    else {
      LOGGER.debug("[{}] - manager event: {}", state.name(), event.name());
    }

    return event;
  }

  @Override
  public void transition(SparkCluster crd, ManagerEvent event) {
    switch (state) {
      case MANAGER_READY:
        break;
      case MANAGER_START_COMMAND:
        if (crd.getStatus().getManager().getStagedCommands().isEmpty()) {
          break;
        }

        String stagedCommand = crd.getStatus().getManager().getStagedCommands().remove(0);
        // set staged to running
        crd.getStatus().getManager().setRunningCommand(
          new SparkClusterStatusCommand.Builder()
            .withCommand(stagedCommand)
            .withStartedAt(String.valueOf(System.currentTimeMillis()))
            .withStatus(SparkManagerCommandState.STARTED.toString())
            .build()
        );
        controller.getCrdClient().updateStatus(crd);
        break;
      case MANAGER_IMAGE_NOT_UPDATED:
        // no image updated
        if (crd.getStatus().getImage().getName().equals(crd.getSpec().getImage())) {
          // set status failed
          crd.getStatus().getManager().getRunningCommand().setStatus(SparkManagerCommandState.FAILED.name());
          crd.getStatus().getManager().getRunningCommand().setReason("No updated image available!");
          controller.getCrdClient().updateStatus(crd);
          LOGGER.warn("[{}] - no updated image available: command {}",
            state, SparkManagerCommandState.FAILED.name());
        }
        break;
      case MANAGER_JOBS_FINISHED:
        // TODO: check if all spark jobs are finished
        if (true /* all spark jobs finished */) {
          // set staged to running
          crd.getStatus().getManager().getRunningCommand().setStatus(SparkManagerCommandState.RUNNING.name());
          controller.getCrdClient().updateStatus(crd);
          // delete all pods
          List<Pod> deletedPods = controller.deleteAllPods(crd, crd.getSpec().getMaster(), crd.getSpec().getWorker());

          LOGGER.debug("[{}] - deleted {} pod(s): {}",
            state, deletedPods.size(), SparkClusterController.metadataListToDebug(deletedPods));
        }
        break;
      case MANAGER_PODS_DELETED:
        // set status running command to finished
        SparkClusterStatusCommand runningCommand = crd.getStatus().getManager().getRunningCommand();
        runningCommand.setStatus(SparkManagerCommandState.FINISHED.toString());
        // set running command finished timestamp
        runningCommand.setFinishedAt(String.valueOf(System.currentTimeMillis()));
        crd.getStatus().getManager().setRunningCommand(runningCommand);

        controller.getCrdClient().updateStatus(crd);
        // TODO: reset state in cluster?
        break;
      case MANAGER_STOPPED:
        // wait for START
        break;
    }
  }

  /**
   * Manager State Machine:
   *                        |
   *                        v
   * +<---------------- MANAGER_READY <-----------------+
   * | image not              | start command			      |
   * | updated                v						        ready |
   * |	          +<--- MANAGER_START_COMMAND <---------+
   * |            |           |	not updated			        ^
   * v            v           v	          start command	|
   * +----------->+---> MANAGER_IMAGE_NOT_UPDATED ----->+
   * | jobs       | jobs                                ^
   * v finished		v finished 	           					 			|
   * +----------->+---> MANAGER_JOBS_FINISHED			      |
   * | pods				| pods      | pods deleted			      |
   * v deleted		v deleted   v				       			ready	|
   * +----------->+---> MANAGER_PODS_DELETED ---------->+
   * |				 				        ^                         ^
   * v stop				    	  	  |                   ready |
   * +----------------> MANAGER_STOPPED --------------->+
   */
  public enum ManagerState {
    // states with their accepted transitions
    MANAGER_READY(
      ManagerEvent.START_COMMAND,
      ManagerEvent.STOP_COMMAND,
      ManagerEvent.JOBS_FINISHED,
      ManagerEvent.PODS_DELETED,
      ManagerEvent.IMAGE_NOT_UPDATED
    ),
    MANAGER_START_COMMAND(
      ManagerEvent.JOBS_FINISHED,
      ManagerEvent.PODS_DELETED,
      ManagerEvent.IMAGE_NOT_UPDATED
    ),
    MANAGER_IMAGE_NOT_UPDATED(
      ManagerEvent.START_COMMAND,
      ManagerEvent.READY
    ),
    MANAGER_JOBS_FINISHED(
      ManagerEvent.PODS_DELETED
    ),
    MANAGER_PODS_DELETED(
      ManagerEvent.READY
    ),
    MANAGER_STOPPED(
      ManagerEvent.READY
    );

    private final List<ManagerEvent> events;
    private static final Map<ManagerEvent, ManagerState> TRANSITIONS = new EnumMap<>(ManagerEvent.class);

    ManagerState(ManagerEvent... in) {
      events = Arrays.asList(in);
    }

    private ManagerState nextState(ManagerEvent event, ManagerState current) {
      ManagerState newState = current;
      if (events.contains(event)) {
        newState = TRANSITIONS.getOrDefault(event, current);
      }
      LOGGER.trace("[{} ==> {}]", current.name(), newState.name());
      return newState;
    }

    // transitions (event,newState)
    static {
      TRANSITIONS.put(ManagerEvent.START_COMMAND, MANAGER_START_COMMAND);
      TRANSITIONS.put(ManagerEvent.IMAGE_NOT_UPDATED, MANAGER_IMAGE_NOT_UPDATED);
      TRANSITIONS.put(ManagerEvent.JOBS_FINISHED, MANAGER_JOBS_FINISHED);
      TRANSITIONS.put(ManagerEvent.PODS_DELETED, MANAGER_PODS_DELETED);
      TRANSITIONS.put(ManagerEvent.STOP_COMMAND, MANAGER_STOPPED);
      TRANSITIONS.put(ManagerEvent.READY, MANAGER_READY);
    }
  }

  /**
   * Systemd state transition events
   */
  public enum ManagerEvent {
    /**
     * manager ready event
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

  public ManagerState getState() {
    return state;
  }

}
