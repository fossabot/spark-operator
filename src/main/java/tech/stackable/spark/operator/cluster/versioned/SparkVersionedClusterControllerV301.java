package tech.stackable.spark.operator.cluster.versioned;

import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.crd.SparkNodeSelector;
import tech.stackable.spark.operator.cluster.versioned.config.SparkConfigVersion;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;
import tech.stackable.spark.operator.common.type.SparkConfig;

public class SparkVersionedClusterControllerV301 extends SparkVersionedClusterController {

  protected SparkVersionedClusterControllerV301(SparkConfigVersion version, KubernetesClient client, Lister<Pod> podLister, Lister<SparkCluster> crdLister,
    MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> crdClient) {
    super(version, client, podLister, crdLister, crdClient);
  }

  @Override
  protected void createMasterConfigMaps(Map<String,String> configProperties, Map<String,String> envVariables, SparkCluster cluster, SparkNodeSelector selector) {
    // add to config properties

    // add to env variables
  }

  @Override
  protected void createWorkerConfigMaps(Map<String,String> configProperties, Map<String,String> envVariables, SparkCluster cluster, SparkNodeSelector selector) {
    // add to config properties
    configProperties.put(SparkConfig.SPARK_WORKER_CORES.getConfig(), selector.getCores());
    configProperties.put(SparkConfig.SPARK_WORKER_MEMORY.getConfig(), selector.getMemory());

    // add to env variables
  }

  @Override
  protected void createHistoryServerConfigMaps(Map<String,String> configProperties, Map<String,String> envVariables, SparkCluster cluster, SparkNodeSelector selector) {
    // add to config properties
    // add to env variables
  }

  @Override
  protected Map<String,String> createCommonConfigProperties(SparkCluster cluster) {
    Map<String,String> configProperties = new HashMap<>();
    // secret?
    String secret = cluster.getSpec().getSecret();
    if(secret != null && !secret.isEmpty()) {
      configProperties.put(SparkConfig.SPARK_AUTHENTICATE.getConfig(), "true");
      configProperties.put(SparkConfig.SPARK_AUTHENTICATE_SECRET.getConfig(), secret);
    }

    // TODO: ssl?

    // TODO: history server logging?

    return configProperties;
  }

  @Override
  protected Map<String,String> createCommonEnvVariables(SparkCluster cluster) {
    Map<String,String> envVarsProperties = new HashMap<>();

    // SPARK_NO_DAEMONIZE
    envVarsProperties.put(SparkConfig.SPARK_NO_DAEMONIZE.name(), "true");
    // SPARK_CONF_DIR
    envVarsProperties.put(SparkConfig.SPARK_CONF_DIR.name(), "{{configroot}}/conf");

    return envVarsProperties;
  }

}
