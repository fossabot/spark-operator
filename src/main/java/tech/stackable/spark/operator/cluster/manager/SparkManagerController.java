package tech.stackable.spark.operator.cluster.manager;

import java.util.List;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatus;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusManager;
import tech.stackable.spark.operator.cluster.manager.crd.SparkManager;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;
import tech.stackable.spark.operator.common.fabric8.SparkSystemdDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkSystemdList;

/**
 * The spark manager controller is responsible for cluster restarts, start and stops.
 * Signal the spark cluster controller of pending restarts via editing the respective SparkCluster status
 */
public class SparkManagerController extends AbstractCrdController<SparkManager, SparkSystemdList, SparkSystemdDoneable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkManagerController.class);

  private final String clusterCrdPath;

  public SparkManagerController(KubernetesClient client, String crdPath, String clusterCrdPath, Long resyncCycle) {
    super(client, crdPath, resyncCycle);
    this.clusterCrdPath = clusterCrdPath;
  }

  @Override
  public void process(SparkManager crd) {
    LOGGER.trace("Got CRD: {}", crd.getMetadata().getName());
    // get custom crd client
    List<HasMetadata> metadata = loadYaml(clusterCrdPath);

    MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> clusterCrdClient =
      getClient().customResources(getCrdContext(metadata), SparkCluster.class, SparkClusterList.class, SparkClusterDoneable.class);

    SparkCluster cluster = clusterCrdClient.inNamespace(getNamespace()).withName(crd.getSpec().getSparkClusterReference()).get();

    // status available?
    SparkClusterStatus status = cluster == null ? null : cluster.getStatus();

    if (status == null) {
      status = new SparkClusterStatus();
    }
    // no staged commands available
    if (status.getManager() == null) {
      status.setManager(new SparkClusterStatusManager.Builder()
        .withSingleStagedCommand(crd.getSpec().getManagerAction())
        .build()
      );
    }
    // already existing staged commands
    else {
      List<String> stagedCommands = status.getManager().getStagedCommands();
      stagedCommands.add(crd.getSpec().getManagerAction());
    }

    if (cluster != null) {
      cluster.setStatus(status);
    }

    try {
      // update status
      clusterCrdClient.updateStatus(cluster);
    } catch (KubernetesClientException ex) {
      LOGGER.warn("Received outdated object: {}", ex.getMessage());
    }
    // remove manager crd?
    if (getCrdClient().inNamespace(getNamespace()).withName(crd.getMetadata().getName()).delete()) {
      LOGGER.trace("deleted manager crd: {} with action: {}", crd.getMetadata().getName(), crd.getSpec().getManagerAction());
    }
  }

}
