package tech.stackable.spark.operator.cluster;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine;
import tech.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine.ClusterEvent;
import tech.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine.ClusterState;
import tech.stackable.spark.operator.cluster.statemachine.SparkManagerStateMachine;
import tech.stackable.spark.operator.cluster.statemachine.SparkManagerStateMachine.ManagerEvent;
import tech.stackable.spark.operator.cluster.statemachine.SparkManagerStateMachine.ManagerState;
import tech.stackable.spark.operator.cluster.statemachine.SparkStateMachine;
import tech.stackable.spark.operator.cluster.versioned.SparkVersionedClusterController;
import tech.stackable.spark.operator.cluster.versioned.SparkVersionedClusterControllerFactory;
import tech.stackable.spark.operator.cluster.versioned.SparkVersionedClusterControllerFactory.SparkSupportedVersion;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;

/**
 * The SparkClusterController is responsible for installing the spark master, worker and history server.
 * Scale up and down to the instances required in the specification.
 * Offer functionality for start, stop, restart and update the cluster
 */
public class SparkClusterController extends AbstractCrdController<SparkCluster, SparkClusterList, SparkClusterDoneable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkClusterController.class);

  private SharedIndexInformer<Pod> podInformer;
  private Lister<Pod> podLister;

  private final SparkStateMachine<SparkCluster, ManagerEvent, ManagerState> managerStateMachine;
  private final SparkStateMachine<SparkCluster, ClusterEvent, ClusterState> clusterStateMachine;

  private SparkSupportedVersion currentVersion = SparkSupportedVersion.NOT_SUPPORTED;
  private SparkVersionedClusterController versionedController;

  public SparkClusterController(KubernetesClient client, String crdPath, Long resyncCycle) {
    super(client, crdPath, resyncCycle);

    managerStateMachine = new SparkManagerStateMachine();
    clusterStateMachine = new SparkClusterStateMachine();
  }

  @Override
  public void init() {
    super.init();

    podInformer = getInformerFactory().sharedIndexInformerFor(Pod.class, PodList.class, getResyncCycle());
    podLister = new Lister<>(podInformer.getIndexer(), getNamespace());
    // register pod event handler
    registerPodEventHandler();
  }

  @Override
  protected void waitForAllInformersSynced() {
    while (!getCrdSharedIndexInformer().hasSynced() || !podInformer.hasSynced()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted during informer sync: {}", e.getMessage());
      }
    }
    LOGGER.info("SparkCluster informer and pod informer initialized ... waiting for changes");
  }

  @Override
  public void process(SparkCluster crd) {
    // TODO: check which controller version to use
    SparkSupportedVersion receivedVersion = SparkSupportedVersion.getVersionType(crd.getSpec().getImage(),2);
    if(currentVersion != receivedVersion && receivedVersion != SparkSupportedVersion.NOT_SUPPORTED) {
      currentVersion = receivedVersion;
      versionedController = SparkVersionedClusterControllerFactory
          .getSparkVersionedController(crd.getSpec().getImage(), getClient(), podLister, getCrdLister(), getCrdClient());
      // adapt all state machines
      managerStateMachine.setVersionedController(versionedController);
      clusterStateMachine.setVersionedController(versionedController);

      LOGGER.trace("Switch controller to version: {}", receivedVersion.name());
    }

    // TODO: except stopped?
    // only go for cluster state machine if no systemd action is currently running
    if (managerStateMachine.process(crd)) {
      return;
    }
    clusterStateMachine.process(crd);
  }

  /**
   * Register event handler for kubernetes pods
   */
  private void registerPodEventHandler() {
    podInformer.addEventHandler(new ResourceEventHandler<>() {
      @Override
      public void onAdd(Pod pod) {
        LOGGER.trace("onAddPod: {}", pod);
        handlePodObject(pod);
      }

      @Override
      public void onUpdate(Pod oldPod, Pod newPod) {
        if (oldPod.getMetadata().getResourceVersion().equals(newPod.getMetadata().getResourceVersion())) {
          return;
        }
        LOGGER.trace("onUpdate:\npodOld: {}\npodNew: {}", oldPod, newPod);
        handlePodObject(newPod);
      }

      @Override
      public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
        LOGGER.trace("onDeletePod: {}", pod);
      }
    });
  }

  /**
   * Check incoming pods for owner reference of SparkCluster and add to blocking queue
   *
   * @param pod kubernetes pod
   */
  private void handlePodObject(Pod pod) {
    SparkCluster cluster = versionedController.podInCluster(pod, getCrdContext().getKind());
    if (cluster != null) {
      enqueueCrd(cluster);
    }
  }

  public SparkStateMachine<SparkCluster, ManagerEvent, ManagerState> getManagerStateMachine() {
    return managerStateMachine;
  }

  public SparkStateMachine<SparkCluster, ClusterEvent, ClusterState> getClusterStateMachine() {
    return clusterStateMachine;
  }

  public Lister<Pod> getPodLister() {
    return podLister;
  }
}
