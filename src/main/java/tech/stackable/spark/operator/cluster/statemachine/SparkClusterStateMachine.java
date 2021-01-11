package tech.stackable.spark.operator.cluster.statemachine;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatus;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusImage;
import tech.stackable.spark.operator.cluster.crd.SparkNode;
import tech.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine.ClusterEvent;
import tech.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine.ClusterState;
import tech.stackable.spark.operator.cluster.versioned.SparkVersionedClusterController;
import tech.stackable.spark.operator.cluster.versioned.SparkVersionedClusterControllerHelper;
import tech.stackable.spark.operator.common.state.PodState;
import tech.stackable.spark.operator.common.state.SparkManagerCommand;
import tech.stackable.spark.operator.common.state.SparkManagerCommandState;

public class SparkClusterStateMachine implements SparkStateMachine<SparkCluster, ClusterEvent, ClusterState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkClusterStateMachine.class);

  private ClusterState state;
  private SparkVersionedClusterController controller;

  public SparkClusterStateMachine() {
    state = ClusterState.READY;
  }

  @Override
  public boolean process(SparkCluster crd) {
    ClusterEvent event = transition(crd);
    if (event != ClusterEvent.WAIT) {
      doAction(crd, event);
      return true;
    }
    return false;
  }

  /**
   * Reconcile the spark cluster. Compare current with desired state and adapt via events
   *
   * @param crd current spark cluster
   *
   * @return ClusterEvent:
   * INITIAL if no status found or command running which is not stop
   * CREATE_MASTER if #masters < spec;
   * WAIT_HOST_NAME if masters created but not running
   * WAIT_MASTER_RUNNING if host names received and config map written
   * CREATE_WORKER if #workers < spec;
   * WAIT_WORKER_RUNNING if workers created but not running yet
   * READY if cluster is in desired state
   */
  @Override
  public ClusterEvent transition(SparkCluster crd) {
    SparkNode master = crd.getSpec().getMaster();
    SparkNode worker = crd.getSpec().getWorker();
    List<Pod> masterPods = controller.getPodsByNode(crd, master);
    List<Pod> workerPods = controller.getPodsByNode(crd, worker);

    // delete excess master pods
    // TODO: leader?
    if (SparkVersionedClusterControllerHelper.getPodSpecToClusterDifference(master, masterPods) < 0) {
      List<Pod> deletedPods = controller.deletePods(masterPods, crd, master);
      LOGGER.debug("[{}] - deleted {} {} pod(s): {}",
        state.name(), deletedPods.size(), master.getNodeType(), SparkVersionedClusterControllerHelper.metadataListToDebug(deletedPods));
    }
    // delete excess worker pods
    if (SparkVersionedClusterControllerHelper.getPodSpecToClusterDifference(worker, workerPods) < 0) {
      List<Pod> deletedPods = controller.deletePods(workerPods, crd, worker);
      LOGGER.debug("[{}] - deleted {} {} pod(s): {}",
        state.name(), deletedPods.size(), worker.getNodeType(), SparkVersionedClusterControllerHelper.metadataListToDebug(deletedPods));
    }

    // run for initial state
    // status not null or
    ClusterEvent event = ClusterEvent.READY;
    if (crd.getStatus() == null) {
      event = ClusterEvent.INITIAL;
    }
    // running command not null and status finished and command not stop
    else if (crd.getStatus().getManager() != null
            && crd.getStatus().getManager().getRunningCommand() != null
            && crd.getStatus().getManager().getRunningCommand().getStatus().equals(SparkManagerCommandState.FINISHED.toString())
            && SparkManagerCommand.getManagerCommand(crd.getStatus().getManager().getRunningCommand().getCommand())
            != SparkManagerCommand.STOP) {
      event = ClusterEvent.INITIAL;
    }
    // masters missing
    else if (SparkVersionedClusterControllerHelper.getPodSpecToClusterDifference(master, masterPods) > 0) {
      event = ClusterEvent.CREATE_MASTER;
    } else if (SparkVersionedClusterControllerHelper.getHostNames(masterPods).isEmpty()) {
      // got host name?
      // TODO who is leader?
      event = ClusterEvent.WAIT;
    }
    // config maps not written yet
    else if (SparkVersionedClusterControllerHelper.getHostNames(masterPods).size() != 0
      && controller.getConfigMaps(masterPods, crd).size() != masterPods.size()) {
      // TODO who is leader?
      event = ClusterEvent.WAIT_MASTER_HOST_NAME;
    }
    // not all masters running
    else if (!SparkVersionedClusterControllerHelper.allPodsHaveStatus(masterPods, PodState.RUNNING)) {
      event = ClusterEvent.WAIT_MASTER_RUNNING;
    }
    // masters running - workers missing
    else if (SparkVersionedClusterControllerHelper.getPodSpecToClusterDifference(worker, workerPods) > 0) {
      event = ClusterEvent.CREATE_WORKER;
    }
    // config maps not written yet
    else if (SparkVersionedClusterControllerHelper.getHostNames(workerPods).size() != 0
      && controller.getConfigMaps(workerPods, crd).size() != workerPods.size()) {
      // TODO who is leader?
      event = ClusterEvent.WAIT_WORKER_HOST_NAME;
    }
    // all masters running - not all workers running
    else if (!SparkVersionedClusterControllerHelper.allPodsHaveStatus(workerPods, PodState.RUNNING)) {
      event = ClusterEvent.WAIT_WORKER_RUNNING;
    }

    ClusterState old = state;
    state = state.nextState(event, state);
    // no changes if not initial
    if (old == state && old != ClusterState.INITIAL) {
      event = ClusterEvent.WAIT;
    }
    // else log
    else {
      LOGGER.debug("[{}] - {} [{} / {}] | {} [{} / {}]",
        state.name(),
        master.getNodeType(), masterPods.size(), master.getInstances(),
        worker.getNodeType(), workerPods.size(), worker.getInstances()
      );
    }
    return event;
  }

  /**
   * Handle events and execute given states
   *
   * @param crd spark cluster
   * @param event given event
   */
  @Override
  public void doAction(SparkCluster crd, ClusterEvent event) {
    try {
      switch (state) {
        case INITIAL:
          // check for available status or create new one
          SparkClusterStatus status = crd.getStatus() != null ? crd.getStatus() : new SparkClusterStatus();
          // add spark image (deployedImage) to status for manager update
          status.setImage(
            new SparkClusterStatusImage(crd.getSpec().getImage(), String.valueOf(System.currentTimeMillis()))
          );
          // TODO: reset status for manager?
          status.setManager(null);

          crd.setStatus(status);
          // update status
          controller.getCrdClient().updateStatus(crd);
          // remove old config maps
          List<ConfigMap> deletedConfigMaps = controller.deleteAllClusterConfigMaps(crd);

          LOGGER.debug("[{}] - deleted {} configMap(s): {}",
            state.name(), deletedConfigMaps.size(), SparkVersionedClusterControllerHelper.metadataListToDebug(deletedConfigMaps));
          break;
        case CREATE_MASTER: {
          SparkNode master = crd.getSpec().getMaster();
          List<Pod> masterPods = controller.getPodsByNode(crd, master);
          // delete old configmap
          controller.deleteConfigMaps(masterPods, crd);
          // create master instances if required
          masterPods = controller.createPods(masterPods, crd, master);

          LOGGER.debug("[{}] - created {} {} pod(s): {}",
            state.name(), masterPods.size(), master.getNodeType(), SparkVersionedClusterControllerHelper.metadataListToDebug(masterPods));
          break;
        }
        case WAIT_MASTER_HOST_NAME: {
          SparkNode master = crd.getSpec().getMaster();
          List<Pod> masterPods = controller.getPodsByNode(crd, master);
          List<String> masterHostNames = SparkVersionedClusterControllerHelper.getHostNames(masterPods);
          if (masterHostNames.size() != 0) {
            // TODO: actual master?
            LOGGER.debug("[{}] - got host name: {}", state.name(), masterHostNames.get(0));
            // create master config map when nodename received
            List<ConfigMap> createdConfigMaps = controller.createConfigMaps(masterPods, crd, master);
            LOGGER.debug("[{}] - created {} configMap(s): {}",
              state.name(), createdConfigMaps.size(), SparkVersionedClusterControllerHelper.metadataListToDebug(createdConfigMaps));
          }
          break;
        }
        case WAIT_MASTER_RUNNING:
          break;
        case CREATE_WORKER: {
          SparkNode master = crd.getSpec().getMaster();
          List<Pod> masterPods = controller.getPodsByNode(crd, master);
          SparkNode worker = crd.getSpec().getWorker();
          List<Pod> workerPods = controller.getPodsByNode(crd, worker);

          // check host name
          List<String> masterNodeNames = SparkVersionedClusterControllerHelper.getHostNames(masterPods);

          if (masterNodeNames.isEmpty()) {
            LOGGER.warn("[{}] - no master node names available!", state.name());
            break;
          }
          // adapt command in workers
          String masterUrl = SparkVersionedClusterControllerHelper.adaptWorkerCommand(crd, masterNodeNames);
          LOGGER.debug("[{}] - set worker MASTER_URL to: {}", state.name(), masterUrl);

          // spin up workers
          workerPods = controller.createPods(workerPods, crd, worker);

          LOGGER.debug("[{}] - created {} {} pod(s): {}",
            state.name(), workerPods.size(), worker.getNodeType(), SparkVersionedClusterControllerHelper.metadataListToDebug(workerPods));
          break;
        }
        case WAIT_WORKER_HOST_NAME:
          SparkNode worker = crd.getSpec().getWorker();
          List<Pod> workerPods = controller.getPodsByNode(crd, worker);
          List<String> workerHostNames = SparkVersionedClusterControllerHelper.getHostNames(workerPods);
          if (workerHostNames.size() != 0) {
            // TODO: actual master?
            LOGGER.debug("[{}] - got host name: {}", state.name(), workerHostNames.get(0));
            // create master config map when nodename received
            List<ConfigMap> createdConfigMaps = controller.createConfigMaps(workerPods, crd, worker);

            LOGGER.debug("[{}] - created {} configMap(s): {}",
              state.name(), createdConfigMaps.size(), SparkVersionedClusterControllerHelper.metadataListToDebug(createdConfigMaps));
          }
          break;
        case WAIT_WORKER_RUNNING:
          break;
        case READY:
          break;
      }
    } catch (KubernetesClientException ex) {
      LOGGER.warn("Received outdated object: {}", ex.getMessage());
    }
  }

  @Override
  public ClusterState getState() {
    return state;
  }

  @Override
  public void setVersionedController(SparkVersionedClusterController controller) {
    this.controller = controller;
  }

  /**
   * Cluster state machine:
   *                  |
   * ready            v
   * +<--+<-----  CLUSTER_INITIAL
   * |	| 		        | create master
   * |	|       		  v
   * |	|		      CREATE_MASTER <-----------------+
   * |	| create      | wait host name		        ^
   * |	|	worker      |							              |
   * |	|			        v			        create master	|
   * |	|		      WAIT_MASTER_HOST_NAME --------->+
   * |	|			        | master running			      ^
   * |	|			        |							              |
   * |	|			        v			        create master	|
   * |	|		      WAIT_MASTER_RUNNING ----------->+
   * |	|			        | create worker				      ^
   * |	v			        v							create master |
   * |	+-------> CREATE_WORKER ----------------->+
   * |	^			        | wait worker host name	    ^
   * |	| create	    |							              |
   * |	| worker	    v			        create master	|
   * |	+<------- WAIT_WORKER_HOST_NAME --------->+
   * |	^			        | worker running			      ^
   * |	| create	    |							              |
   * |	| worker	    v			        create master |
   * |	+<------- WAIT_WORKER_RUNNING ----------->+
   * |	^ create	    | ready						          ^
   * |	| worker	    v			        create master	|
   * |	+<------- CLUSTER_READY ----------------->+
   * |				        ^
   * v                |
   * +--------------->+
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
      ClusterEvent.WAIT_MASTER_HOST_NAME
    ),
    /**
     * after masters are created, wait for agent to set the dedicated host name and use for workers
     * ==> create config map
     */
    WAIT_MASTER_HOST_NAME(
      ClusterEvent.CREATE_MASTER,
      ClusterEvent.WAIT_MASTER_RUNNING,
      ClusterEvent.CREATE_WORKER
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
     */
    CREATE_WORKER(
      ClusterEvent.CREATE_MASTER,
      ClusterEvent.WAIT_WORKER_HOST_NAME
    ),
    /**
     * after workers are created, wait for agent to set the dedicated host name and use for workers
     * ==> create config map
     */
    WAIT_WORKER_HOST_NAME(
      ClusterEvent.CREATE_MASTER,
      ClusterEvent.CREATE_WORKER,
      ClusterEvent.WAIT_WORKER_RUNNING
    ),
    /**
     * Wait for all workers running
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
      ClusterEvent.INITIAL,
      ClusterEvent.CREATE_MASTER,
      ClusterEvent.WAIT_MASTER_HOST_NAME,
      ClusterEvent.WAIT_MASTER_RUNNING,
      ClusterEvent.CREATE_WORKER,
      ClusterEvent.WAIT_WORKER_RUNNING,
      ClusterEvent.WAIT_WORKER_HOST_NAME
    );

    private final List<ClusterEvent> events;
    private static final Map<ClusterEvent, ClusterState> TRANSITIONS = new EnumMap<>(ClusterEvent.class);

    ClusterState(ClusterEvent... in) {
      events = Arrays.asList(in);
    }

    private ClusterState nextState(ClusterEvent event, ClusterState current) {
      ClusterState newState = current;
      if (events.contains(event)) {
        newState = TRANSITIONS.getOrDefault(event, current);
      }
      LOGGER.trace(String.format("[%s ==> %s]", current.name(), newState.name()));
      return newState;
    }

    // transitions (event,newState)
    static {
      TRANSITIONS.put(ClusterEvent.INITIAL, INITIAL);
      TRANSITIONS.put(ClusterEvent.CREATE_MASTER, CREATE_MASTER);
      TRANSITIONS.put(ClusterEvent.WAIT_MASTER_HOST_NAME, WAIT_MASTER_HOST_NAME);
      TRANSITIONS.put(ClusterEvent.WAIT_MASTER_RUNNING, WAIT_MASTER_RUNNING);
      TRANSITIONS.put(ClusterEvent.CREATE_WORKER, CREATE_WORKER);
      TRANSITIONS.put(ClusterEvent.WAIT_WORKER_HOST_NAME, WAIT_WORKER_HOST_NAME);
      TRANSITIONS.put(ClusterEvent.WAIT_WORKER_RUNNING, WAIT_WORKER_RUNNING);
      TRANSITIONS.put(ClusterEvent.READY, READY);
    }
  }

  /**
   * cluster state transition events
   */
  public enum ClusterEvent {
    /**
     * event for initial state for full reset
     */
    INITIAL,
    /**
     * event for creating master
     */
    CREATE_MASTER,
    /*
     * event to wait for master hostname
     */
    WAIT_MASTER_HOST_NAME,
    /**
     * event for all masters running
     */
    WAIT_MASTER_RUNNING,
    /**
     * event to create all workers
     */
    CREATE_WORKER,
    /*
     * event to wait for worker hostname
     */
    WAIT_WORKER_HOST_NAME,
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

}
