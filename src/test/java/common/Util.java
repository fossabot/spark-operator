package common;

import java.util.List;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.SparkClusterController;
import tech.stackable.spark.operator.cluster.crd.SparkNode;
import tech.stackable.spark.operator.common.state.PodState;
import tech.stackable.spark.operator.common.type.SparkOperatorConfig;
import tech.stackable.spark.operator.cluster.manager.crd.SparkManager;
import tech.stackable.spark.operator.cluster.manager.SparkManagerController;

public final class Util {
  public static final String CLUSTER_CRD_PATH = "cluster/spark-cluster-crd.yaml";
  public static final String CLUSTER_EXAMPLE_PATH = "cluster/spark-cluster-example.yaml";
  public static final String MANAGER_CRD_PATH = "manager/spark-manager-crd.yaml";
  public static final String MANAGER_EXAMPLE_PATH = "manager/spark-manager-example.yaml";
  public static final String IMAGE_VERSION_3_0_1 = "spark:3.0.1";
  public static final long RESYNC_CYCLE = 60 * 1000L;

  private Util() {}

  public static SparkCluster loadSparkClusterExample(KubernetesClient client, SparkClusterController controller, String crdPath) {
    SparkCluster cluster = controller
      .getCrdClient()
      .load(Thread.currentThread()
      .getContextClassLoader()
      .getResourceAsStream(crdPath))
      .create();
    cluster.getMetadata().setUid("123456789");
    cluster.getMetadata().setNamespace(client.getNamespace());

    return cluster;
  }

  public static SparkManager loadSparkManagerExample(KubernetesClient client, SparkManagerController controller, String crdPath) {
    SparkManager manager = controller
      .getCrdClient()
      .load(Thread.currentThread()
        .getContextClassLoader()
        .getResourceAsStream(crdPath))
      .create();
    manager.getMetadata().setUid("123456789");
    manager.getMetadata().setNamespace(client.getNamespace());

    return manager;
  }

  public static void setNodeNames(List<Pod> pods, SparkNode node) {
    if (node.getSelectors() != null && node.getSelectors().size() > 0) {
      String nodeName = node.getSelectors().get(0).getMatchLabels().get(SparkOperatorConfig.KUBERNETES_IO_HOSTNAME.toString());
      for (Pod pod : pods) {
        pod.getSpec().setNodeName(nodeName);
      }
    }
  }

  public static void setNodeName(KubernetesClient client, List<Pod> pods, SparkCluster cluster) {
    for (Pod pod : pods) {
      pod.getSpec().setNodeName(cluster.getSpec().getMaster().getSelectors().get(0)
        .getMatchLabels().get(SparkOperatorConfig.KUBERNETES_IO_HOSTNAME.toString()));
      // update pods with node name
      client.pods().createOrReplace(pod);
    }
  }

  public static void setStatusRunning(KubernetesClient client, List<Pod> pods) {
    for (Pod pod : pods) {
      PodStatus status = pod.getStatus();
      if (status == null) {
        status = new PodStatus();
        status.setPhase(PodState.RUNNING.toString());
        pod.setStatus(status);
      }
      // update pods with status running
      client.pods().updateStatus(pod);
    }
  }

}
