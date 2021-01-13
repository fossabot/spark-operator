package tech.stackable.spark.operator.cluster.versioned;

import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.crd.SparkNode;
import tech.stackable.spark.operator.cluster.crd.SparkNodeSelector;
import tech.stackable.spark.operator.cluster.versioned.config.SparkConfigVersion;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;

public class SparkVersionedClusterControllerV247 extends SparkVersionedClusterController {

  protected SparkVersionedClusterControllerV247(SparkConfigVersion version, KubernetesClient client, Lister<Pod> podLister, Lister<SparkCluster> crdLister,
    MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> crdClient) {
    super(version, client, podLister, crdLister, crdClient);
  }

  @Override
  protected void createMasterConfigMaps(Map<String,String> configProperties, Map<String,String> envVariables, SparkCluster cluster, SparkNodeSelector selector) {
  }

  @Override
  protected void createWorkerConfigMaps(Map<String,String> configProperties, Map<String,String> envVariables, SparkCluster cluster, SparkNodeSelector selector) {
  }

  @Override
  protected void createHistoryServerConfigMaps(Map<String,String> configProperties, Map<String,String> envVariables, SparkCluster cluster, SparkNodeSelector selector) {
  }

  @Override
  protected Map<String, String> createCommonConfigProperties(SparkCluster cluster) {
    return null;
  }

  @Override
  protected Map<String, String> createCommonEnvVariables(SparkCluster cluster) {
    return null;
  }

}
