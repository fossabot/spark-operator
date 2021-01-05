package tech.stackable.spark.operator.cluster.versioned;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.crd.SparkNode;
import tech.stackable.spark.operator.cluster.crd.SparkNode.SparkNodeType;
import tech.stackable.spark.operator.cluster.crd.SparkNodeSelector;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;
import tech.stackable.spark.operator.common.type.SparkConfig;
import tech.stackable.spark.operator.common.type.SparkOperatorConfig;

public class SparkVersionedClusterControllerV301 extends SparkVersionedClusterController {

  protected SparkVersionedClusterControllerV301(KubernetesClient client, Lister<Pod> podLister, Lister<SparkCluster> crdLister,
    MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> crdClient) {
    super(client, podLister, crdLister, crdClient);
  }

  @Override
  public Pod createPod(SparkCluster cluster, SparkNode node, SparkNodeSelector selector) {
    String podName = createPodName(cluster, node, true);
    String cmName = podName + "-cm";
    ConfigMapVolumeSource cms = new ConfigMapVolumeSourceBuilder().withName(cmName).build();
    Volume vol = new VolumeBuilder().withName(cmName).withConfigMap(cms).build();

    return new PodBuilder()
      .withNewMetadata()
        .withName(podName)
        .withNamespace(cluster.getMetadata().getNamespace())
        .withLabels(Collections.singletonMap(SparkOperatorConfig.POD_SELECTOR_NAME.toString(), selector.getName()))
        .addNewOwnerReference()
          .withController(true)
          .withApiVersion(cluster.getApiVersion())
          .withKind(cluster.getKind())
          .withName(cluster.getMetadata().getName())
          .withNewUid(cluster.getMetadata().getUid())
        .endOwnerReference()
      .endMetadata()
      .withNewSpec()
        .withTolerations(cluster.getSpec().getTolerations())
        // TODO: check for null / zero elements
        .withNodeSelector(selector.getMatchLabels())
        .withVolumes(vol)
          .addNewContainer()
          //TODO: no ":" etc in withName
          .withName("spark-3-0-1")
          .withImage(cluster.getSpec().getImage())
          .withCommand(node.getCommands())
          .withArgs(node.getArgs())
          .addNewVolumeMount()
          .withMountPath(SparkOperatorConfig.POD_CONF_VOLUME_MOUNT_PATH.toString())
          .withName(cmName)
          .endVolumeMount()
          .withEnv(List.copyOf(node.getEnv()))
        .endContainer()
      .endSpec()
      .build();
  }

  @Override
  public List<ConfigMap> createConfigMaps(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    List<ConfigMap> createdConfigMaps = new ArrayList<>();
    // match selector
    Map<Pod, SparkNodeSelector> matchPodToSelectors = getSelectorsForPod(pods, node);

    for (Entry<Pod, SparkNodeSelector> entry : matchPodToSelectors.entrySet()) {
      String cmName = createConfigMapName(entry.getKey());

      Resource<ConfigMap, DoneableConfigMap> configMapResource = getClient()
        .configMaps()
        .inNamespace(cluster.getMetadata().getNamespace())
        .withName(cmName);
      //
      // create entry for spark-env.sh
      //
      StringBuffer sbEnv = new StringBuffer();
      // only worker has required information to be set
      // all known data in yaml and pojo for worker
      if (node.getNodeType() == SparkNodeType.WORKER) {
        sbEnv.append(convertToSparkEnv(SparkConfig.SPARK_WORKER_CORES, entry.getValue().getCores()));
        sbEnv.append(convertToSparkEnv(SparkConfig.SPARK_WORKER_MEMORY, entry.getValue().getMemory()));
      }

      Map<String, String> cmFiles = new HashMap<>();
      cmFiles.put("spark-env.sh", sbEnv.toString());
      //
      // create entry for spark-defaults.conf
      //
      // add secret
      String secret = cluster.getSpec().getSecret();
      if (secret != null && !secret.isEmpty()) {
        node.getSparkConfiguration().add(new EnvVar(SparkConfig.SPARK_AUTHENTICATE.getConfig(), "true", null));
        node.getSparkConfiguration().add(new EnvVar(SparkConfig.SPARK_AUTHENTICATE_SECRET.getConfig(), secret, null));
      }
      StringBuffer sbConf = new StringBuffer();
      sbConf.append(convertToSparkConfig(node.getSparkConfiguration()));

      cmFiles.put("spark-defaults.conf", sbConf.toString());
      //
      // create config map
      //
      ConfigMap created = configMapResource.createOrReplace(new ConfigMapBuilder()
        .withNewMetadata()
        .withName(cmName)
        .endMetadata()
        .addToData(cmFiles)
        .build());
      if (created != null) {
        createdConfigMaps.add(created);
      }
    }

    return createdConfigMaps;
  }

}
