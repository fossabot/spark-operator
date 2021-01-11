package tech.stackable.spark.operator.cluster.versioned;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.crd.SparkNode;
import tech.stackable.spark.operator.cluster.crd.SparkNodeSelector;
import tech.stackable.spark.operator.common.state.PodState;
import tech.stackable.spark.operator.common.type.SparkConfig;
import tech.stackable.spark.operator.common.type.SparkOperatorConfig;

public final class SparkVersionedClusterControllerHelper {

  private SparkVersionedClusterControllerHelper() {}

  /**
   * Adapt worker starting command with master node names as argument
   *
   * @param cluster         spark cluster
   * @param masterNodeNames list of available master nodes
   */
  public static String adaptWorkerCommand(SparkCluster cluster, List<String> masterNodeNames) {
    String masterUrl = null;
    // adapt command in workers
    List<String> commands = cluster.getSpec().getWorker().getCommands();

    if (commands.size() == 1) {
      String port = "7077";
      // if different port in env
      for (EnvVar envVar : cluster.getSpec().getWorker().getEnv()) {
        if (envVar.getName().equals("SPARK_MASTER_PORT")) {
          port = envVar.getValue();
        }
      }

      StringBuilder sb = new StringBuilder();
      sb.append("spark://");
      for (int i = 0; i < masterNodeNames.size(); i++) {
        sb.append(masterNodeNames.get(i))
            .append(":")
            .append(port);
        if (i < masterNodeNames.size() - 1) {
          sb.append(",");
        }
      }

      masterUrl = sb.toString();
      commands.add(masterUrl);
    }
    return masterUrl;
  }

  /**
   * Check if all given pods have status "Running"
   *
   * @param pods   list of pods
   * @param status PodStatus to compare to
   *
   * @return true if all pods have status from pod status
   */
  public static boolean allPodsHaveStatus(List<Pod> pods, PodState status) {
    boolean result = true;
    for (Pod pod : pods) {
      if (pod.getStatus() == null) {
        return false;
      }

      if (!pod.getStatus().getPhase().equals(status.toString())) {
        result = false;
      }
    }
    return result;
  }

  /**
   * Add spark environment variables to string buffer for spark-env.sh configuration
   *
   * @param config key
   * @param value  value
   *
   * @return string in format "key=value\n"
   */
  public static String convertToSparkEnv(SparkConfig config, String value) {
    StringBuilder sb = new StringBuilder();
    if (value != null && !value.isEmpty()) {
      sb.append(config.toEnv()).append("=").append(value).append("\n");
    }
    return sb.toString();
  }

  /**
   * Add to string buffer for spark configuration (spark-default.conf) in config map
   *
   * @param sparkConfiguration spark config given in specs
   *
   * @return string in format "key value\n"
   */
  public static String convertToSparkConfig(Set<EnvVar> sparkConfiguration) {
    StringBuilder sb = new StringBuilder();
    for (EnvVar envVar : sparkConfiguration) {
      String name = envVar.getName();
      String value = envVar.getValue();
      if (name != null && !name.isEmpty() && value != null && !value.isEmpty()) {
        sb.append(name).append(" ").append(value).append("\n");
      }
    }
    return sb.toString();
  }

  /**
   * Create config map name for given pod
   *
   * @param pod pod with config map
   *
   * @return config map name
   */
  public static String createConfigMapName(Pod pod) {
    return pod.getMetadata().getName() + "-cm";
  }

  /**
   * Create pod name schema
   *
   * @param cluster  current spark cluster
   * @param node     master or worker node
   * @param withUUID if true append generated UUID, if false keep generated name
   *
   * @return pod name
   */
  public static String createPodName(SparkCluster cluster, SparkNode node, boolean withUUID) {
    String podName = cluster.getMetadata().getName() + "-" + node.getNodeType() + "-";
    if (withUUID) {
      podName += UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
    return podName;
  }

  /**
   * Extract leader node name from pods
   *
   * @param pods list of master pods
   *
   * @return null or pod.spec.nodeName if available
   */
  public static List<String> getHostNames(List<Pod> pods) {
    // TODO: determine master leader
    List<String> nodeNames = new ArrayList<>();
    for (Pod pod : pods) {
      String nodeName = pod.getSpec().getNodeName();
      if (nodeName != null && !nodeName.isEmpty()) {
        nodeNames.add(nodeName);
      }
    }
    return nodeNames;
  }

  /**
   * Return difference between cluster specification and current cluster state
   *
   * @return = 0 if specification equals state -> no operation
   * < 0 if state greater than specification -> delete pods
   * > 0 if state smaller specification -> create pods
   */
  public static int getPodSpecToClusterDifference(SparkNode node, List<Pod> pods) {
    return node.getInstances() - pods.size();
  }

  /**
   * retrieve corresponding selector for pod
   *
   * @param pods pod with node / hostname
   * @param node spark master / worker node
   *
   * @return selector corresponding to pod
   */
  public static Map<Pod, SparkNodeSelector> getSelectorsForPod(List<Pod> pods, SparkNode node) {
    Map<Pod, SparkNodeSelector> matchPodOnSelector = new HashMap<>();
    // for each selector
    for (SparkNodeSelector selector : node.getSelectors()) {
      int instances = selector.getInstances();
      // for each pod check if matchlabels is a fit and
      for (Pod pod : pods) {
        // if hostnames match && instances left from spec && pod no selector yet
        if (pod.getSpec().getNodeName().equals(selector.getMatchLabels().get(SparkOperatorConfig.KUBERNETES_IO_HOSTNAME.toString()))
            && instances > 0
            && matchPodOnSelector.get(pod) == null) {
          // add to map
          matchPodOnSelector.put(pod, selector);
          // reduce left over instances
          instances--;
        }
      }
    }
    return matchPodOnSelector;
  }

  /**
   * Helper method to print pod lists
   *
   * @param hasMetadata list of objects to print (implementing) HasMetadata
   *
   * @return pod metadata.name
   */
  public static List<String> metadataListToDebug(List<? extends HasMetadata> hasMetadata) {
    List<String> output = new ArrayList<>();
    hasMetadata.forEach(n -> output.add(n.getMetadata().getName()));
    return output;
  }

  /**
   * Return the owner reference of that specific pod if available
   *
   * @param pod fabric8 pod
   *
   * @return pod owner reference or null
   */
  public static OwnerReference getControllerOf(Pod pod) {
    List<OwnerReference> ownerReferences = pod.getMetadata().getOwnerReferences();
    for (OwnerReference ownerReference : ownerReferences) {
      if (ownerReference.getController().equals(Boolean.TRUE)) {
        return ownerReference;
      }
    }
    return null;
  }

  /**
   * Split existing pods according to spec in selectors and their hostnames
   *
   * @param pods list of existing pods
   * @param node spark node (master/worker)
   *
   * @return map where the key is the hostname and value is a list of pods with that hostname
   */
  public static Map<SparkNodeSelector, List<Pod>> splitPodsBySelector(List<Pod> pods, SparkNode node) {
    Map<SparkNodeSelector, List<Pod>> podsByHost = new HashMap<>();

    // check in each node selector
    for (SparkNodeSelector selector : node.getSelectors()) {
      // check if pod.nodename equals selector.matchlabels.hostname
      String hostName = selector.getMatchLabels().get(SparkOperatorConfig.KUBERNETES_IO_HOSTNAME.toString());

      if (hostName != null) {
        podsByHost.put(selector, new ArrayList<>());
      }

      // each pod
      for (Pod pod : pods) {
        // check if hostname and selector label matches
        if (pod.getSpec().getNodeName().equals(hostName)
            && selector.getName().equals(pod.getMetadata().getLabels().get(SparkOperatorConfig.POD_SELECTOR_NAME.toString()))) {
          podsByHost.get(selector).add(pod);
        }
      }
    }
    return podsByHost;
  }

}
