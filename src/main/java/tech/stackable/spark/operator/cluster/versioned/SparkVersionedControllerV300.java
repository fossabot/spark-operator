package tech.stackable.spark.operator.cluster.versioned;

import java.util.List;
import java.util.Set;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.crd.SparkNode;
import tech.stackable.spark.operator.cluster.crd.SparkNodeSelector;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;
import tech.stackable.spark.operator.common.type.SparkConfig;

public class SparkVersionedControllerV300 extends SparkVersionedController {

  protected SparkVersionedControllerV300(KubernetesClient client, Lister<Pod> podLister, Lister<SparkCluster> crdLister,
    MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> crdClient) {
    super(client, podLister, crdLister, crdClient);
  }

  @Override
  public Pod createPod(SparkCluster cluster, SparkNode node, SparkNodeSelector selector) {
    return null;
  }

  @Override
  public List<ConfigMap> createConfigMaps(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    return null;
  }

  @Override
  public void addToSparkConfig(Set<EnvVar> sparkConfiguration, StringBuffer sb) {

  }

  @Override
  public void addToSparkEnv(StringBuffer sb, SparkConfig config, String value) {

  }
}
