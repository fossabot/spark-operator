package tech.stackable.spark.operator.cluster.versioned;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
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
import tech.stackable.spark.operator.common.state.PodState;
import tech.stackable.spark.operator.common.type.SparkConfig;
import tech.stackable.spark.operator.common.type.SparkOperatorConfig;

public abstract class SparkVersionedController {

  private final KubernetesClient client;
  private final Lister<Pod> podLister;
  private final Lister<SparkCluster> crdLister;
  private final MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> crdClient;

  protected SparkVersionedController(KubernetesClient client, Lister<Pod> podLister, Lister<SparkCluster> crdLister,
    MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> crdClient) {
    this.client = client;
    this.podLister = podLister;
    this.crdLister = crdLister;
    this.crdClient = crdClient;
  }

  /**
   * Create pod for spark node
   *
   * @param cluster spark cluster
   * @param node    master or worker node
   * @param selector respective selector for node to be created
   *
   * @return pod create from specs
   */
  public abstract Pod createPod(SparkCluster cluster, SparkNode node, SparkNodeSelector selector);

  /**
   * Create config map content for master / worker
   *
   * @param pods    pods to create config maps for
   * @param cluster spark cluster
   * @param node    spark master / worker
   *
   * @return list of created configmaps
   */
  public abstract List<ConfigMap> createConfigMaps(List<Pod> pods, SparkCluster cluster, SparkNode node);

  /**
   * Add to string buffer for spark configuration (spark-default.conf) in config map
   *
   * @param sparkConfiguration spark config given in specs
   * @param sb                 string buffer to add to
   */
  public abstract void addToSparkConfig(Set<EnvVar> sparkConfiguration, StringBuffer sb);

  /**
   * Add spark environment variables to string buffer for spark-env.sh configuration
   *
   * @param sb     string buffer to add to
   * @param config key
   * @param value  value
   */
  public abstract void addToSparkEnv(StringBuffer sb, SparkConfig config, String value);

  /**
   * Create pods with regard to spec and current state
   *
   * @param pods    list of available pods belonging to the given node
   * @param cluster current spark cluster
   * @param node    master or worker node
   *
   * @return list of created pods
   */
  public List<Pod> createPods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    List<Pod> createdPods = new ArrayList<>();
    // If less then create new pods
    if (pods.size() < node.getInstances()) {
      // check which host names are missing
      Map<SparkNodeSelector, List<Pod>> podsByHostName = splitPodsBySelector(pods, node);

      for (Entry<SparkNodeSelector, List<Pod>> entry : podsByHostName.entrySet()) {
        SparkNodeSelector selector = entry.getKey();
        // get instances for each selector
        int instances = selector.getInstances();
        List<Pod> podsBySelector = entry.getValue();

        for (int index = 0; index < instances - podsBySelector.size(); index++) {
          Pod pod = client.pods()
            .inNamespace(cluster.getMetadata().getNamespace())
            .create(createPod(cluster, node, selector));
          createdPods.add(pod);
        }
      }
    }
    return createdPods;
  }

  /**
   * Split existing pods according to spec in selectors and their hostnames
   *
   * @param pods list of existing pods
   * @param node spark node (master/worker)
   *
   * @return map where the key is the hostname and value is a list of pods with that hostname
   */
  private static Map<SparkNodeSelector, List<Pod>> splitPodsBySelector(List<Pod> pods, SparkNode node) {
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

  /**
   * retrieve corresponding selector for pod
   *
   * @param pods pod with node / hostname
   * @param node spark master / worker node
   *
   * @return selector corresponding to pod
   */
  private static Map<Pod, SparkNodeSelector> getSelectorsForPod(List<Pod> pods, SparkNode node) {
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
   * Delete pods with regard to spec and current state -> for reconciliation
   *
   * @param pods    list of available pods belonging to the given node
   * @param cluster current spark cluster
   * @param node    master or worker node
   *
   * @return list of deleted pods
   */
  public List<Pod> deleteAllPods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    List<Pod> deletedPods = new ArrayList<>();
    // remember processed pods to delete pods not matching any selector
    List<Pod> processedPods = new ArrayList<>();

    Map<SparkNodeSelector, List<Pod>> podsBySelector = splitPodsBySelector(pods, node);
    // delete pods from selector if not matching spec
    for (Entry<SparkNodeSelector, List<Pod>> entry : podsBySelector.entrySet()) {
      List<Pod> podsInSelector = entry.getValue();
      SparkNodeSelector selector = entry.getKey();
      // add pods to processedPods
      processedPods.addAll(podsInSelector);
      // If more pods than spec delete old pods
      for (int diff = podsInSelector.size() - selector.getInstances(); diff > 0; diff--) {
        // TODO: do not remove current master leader!
        Pod pod = podsInSelector.remove(0);
        client.pods()
          .inNamespace(cluster.getMetadata().getNamespace())
          .withName(pod.getMetadata().getName())
          .delete();
        deletedPods.add(pod);
      }
    }
    // delete pods not matching any selector
    for (Pod pod : pods) {
      // delete if not in processed pods
      if (!processedPods.contains(pod)) {
        client.pods()
          .inNamespace(cluster.getMetadata().getNamespace())
          .withName(pod.getMetadata().getName())
          .delete();
        deletedPods.add(pod);
      }
    }

    return deletedPods;
  }

  /**
   * Delete all node pods in cluster with no regard to spec -> systemd
   *
   * @param cluster specification to retrieve all used pods
   * @param nodes array of nodes to be deleted (master, worker, history-server)
   *
   * @return list of deleted pods
   */
  public List<Pod> deleteAllPods(SparkCluster cluster, SparkNode... nodes) {
    // collect master and worker nodes
    List<Pod> pods = new ArrayList<>();
    for (SparkNode node : nodes) {
      pods.addAll(getPodsByNode(cluster, node));
    }
    // delete pods
    List<Pod> deletedPods = new ArrayList<>();
    for (Pod pod : pods) {
      // delete from cluster
      client.pods()
        .inNamespace(cluster.getMetadata().getNamespace())
        .withName(pod.getMetadata().getName())
        .delete();
      // add to deleted list
      deletedPods.add(pod);
    }

    return deletedPods;
  }

  /**
   * Return number of pods for given nodes - Terminating is excluded
   *
   * @param cluster current spark cluster
   * @param nodes   master, worker or history-server nodes
   *
   * @return list of pods belonging to the given node(s)
   */
  public List<Pod> getPodsByNode(SparkCluster cluster, SparkNode... nodes) {
    List<Pod> podList = new ArrayList<>();
    for (SparkNode node : nodes) {
      String nodeName = createPodName(cluster, node, false);

      List<Pod> pods = podLister.list();
      // not in cache (for testing)
      if (pods.isEmpty()) {
        pods = client.pods().list().getItems();
      }

      for (Pod pod : pods) {
        // filter for pods not belonging to cluster
        if (podInCluster(pod, cluster.getKind()) == null) {
          continue;
        }
        // filter for terminating pods
        if (pod.getMetadata().getDeletionTimestamp() != null) {
          continue;
        }
        // TODO: Filter PodStatus: Running...Failure etc.
        if (pod.getMetadata().getName().contains(nodeName)) {
          podList.add(pod);
        }
      }
    }
    return podList;
  }

  /**
   * Return the owner reference of that specific pod if available
   *
   * @param pod fabric8 pod
   *
   * @return pod owner reference
   */
  private static OwnerReference getControllerOf(Pod pod) {
    List<OwnerReference> ownerReferences = pod.getMetadata().getOwnerReferences();
    for (OwnerReference ownerReference : ownerReferences) {
      if (ownerReference.getController().equals(Boolean.TRUE)) {
        return ownerReference;
      }
    }
    return null;
  }

  /**
   * Check if pod belongs to cluster
   *
   * @param pod pod to be checked for owner reference
   *
   * @return SparkCluster the pod belongs to, otherwise null
   */
  private SparkCluster podInCluster(Pod pod, String kind) {
    OwnerReference ownerReference = getControllerOf(pod);
    if (ownerReference == null) {
      return null;
    }
    // check if pod belongs to spark cluster
    SparkCluster cluster = null;
    if (ownerReference.getKind().equalsIgnoreCase(kind)) {
      cluster = crdLister.get(ownerReference.getName());
      // not in cache (for testing)
      if (cluster == null) {
        List<SparkCluster> sparkClusters = crdClient.list().getItems();
        for (SparkCluster sc : sparkClusters) {
          if (sc.getMetadata().getName().equals(ownerReference.getName())) {
            return sc;
          }
        }
      }
    }
    return cluster;
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
  private static String createPodName(SparkCluster cluster, SparkNode node, boolean withUUID) {
    String podName = cluster.getMetadata().getName() + "-" + node.getNodeType() + "-";
    if (withUUID) {
      podName += UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
    return podName;
  }

  /**
   * Create config map name for pod
   *
   * @param pod pod with config map
   *
   * @return config map name
   */
  private static String createConfigMapName(Pod pod) {
    return pod.getMetadata().getName() + "-cm";
  }


  /**
   * Get config map content for master / worker
   *
   * @param pods    spark master / worker pods
   * @param cluster spark cluster
   *
   * @return list of configmaps for given pods
   */
  public List<ConfigMap> getConfigMaps(List<Pod> pods, SparkCluster cluster) {
    List<ConfigMap> configMaps = new ArrayList<>();
    for (Pod pod : pods) {
      String cmName = createConfigMapName(pod);

      Resource<ConfigMap, DoneableConfigMap> configMapResource = client
        .configMaps()
        .inNamespace(cluster.getMetadata().getNamespace())
        .withName(cmName);

      // get config map
      ConfigMap cm = configMapResource.get();
      if (cm != null) {
        configMaps.add(cm);
      }
    }
    return configMaps;
  }

  /**
   * Delete config map content for pods
   *
   * @param cluster spark cluster
   */
  public void deleteConfigMaps(List<Pod> pods, SparkCluster cluster) {
    for (Pod pod : pods) {
      String cmName = createConfigMapName(pod);

      Resource<ConfigMap, DoneableConfigMap> configMapResource = client
        .configMaps()
        .inNamespace(cluster.getMetadata().getNamespace())
        .withName(cmName);

      // delete
      configMapResource.delete();
    }
  }

  /**
   * delete all config maps associated with cluster
   *
   * @param cluster spark cluster
   *
   * @return list of deleted config maps
   */
  public List<ConfigMap> deleteAllClusterConfigMaps(SparkCluster cluster) {
    List<ConfigMap> deletedConfigMaps = new ArrayList<>();
    List<ConfigMap> configMaps = client
      .configMaps()
      .inNamespace(cluster.getMetadata().getNamespace())
      .list()
      .getItems();

    SparkNode[] nodes = {cluster.getSpec().getMaster(), cluster.getSpec().getWorker()};

    for (SparkNode node : nodes) {
      for (ConfigMap cm : configMaps) {
        if (cm.getMetadata().getName().startsWith(createPodName(cluster, node, false))) {
          client.configMaps().delete(cm);
          deletedConfigMaps.add(cm);
        }
      }
    }
    return deletedConfigMaps;
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
}
