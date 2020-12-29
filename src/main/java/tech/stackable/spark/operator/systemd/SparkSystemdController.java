package tech.stackable.spark.operator.systemd;

import java.util.List;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import tech.stackable.spark.operator.cluster.SparkCluster;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatus;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusSystemd;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;
import tech.stackable.spark.operator.common.fabric8.SparkSystemdDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkSystemdList;

/**
 * The spark systemd controller is responsible for cluster restarts.
 * Signal the spark cluster controller of pending restarts via editing the respective SparkCluster
 */
public class SparkSystemdController extends AbstractCrdController<SparkSystemd, SparkSystemdList, SparkSystemdDoneable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkSystemdController.class);

  private final String clusterCrdPath;

  public SparkSystemdController(KubernetesClient client, String crdPath, String clusterCrdPath, Long resyncCycle) {
    super(client, crdPath, resyncCycle);
    this.clusterCrdPath = clusterCrdPath;
  }

  @Override
  public void process(SparkSystemd crd) {
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
    if (status.getSystemd() == null) {
      status.setSystemd(new SparkClusterStatusSystemd.Builder()
        .withSingleStagedCommand(crd.getSpec().getSystemdAction())
        .build()
      );
    }
    // already existing staged commands
    else {
      List<String> stagedCommands = status.getSystemd().getStagedCommands();
      stagedCommands.add(crd.getSpec().getSystemdAction());
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
    // remove systemd crd?
    if (getCrdClient().inNamespace(getNamespace()).withName(crd.getMetadata().getName()).delete()) {
      LOGGER.trace("deleted systemd crd: {} with action: {}", crd.getMetadata().getName(), crd.getSpec().getSystemdAction());
    }
  }

}
