package tech.stackable.spark.operator.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import tech.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import tech.stackable.spark.operator.cluster.crd.SparkNode;
import tech.stackable.spark.operator.cluster.crd.SparkNodeSelector;
import tech.stackable.spark.operator.cluster.crd.SparkNodeWorker;
import tech.stackable.spark.operator.cluster.statemachine.SparkClusterStateMachine;
import tech.stackable.spark.operator.cluster.statemachine.SparkSystemdStateMachine;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;
import tech.stackable.spark.operator.common.state.PodState;
import tech.stackable.spark.operator.common.type.SparkConfig;
import tech.stackable.spark.operator.common.type.SparkOperatorConfig;

/**
 * The SparkClusterController is responsible for installing the spark master and workers.
 * Scale up and down to the instances required in the specification.
 * Offer systemd functionality for start, stop and restart the cluster
 */
public class SparkClusterController extends AbstractCrdController<SparkCluster, SparkClusterList, SparkClusterDoneable> {

  private static final Logger logger = Logger.getLogger(SparkClusterController.class.getName());

  private SharedIndexInformer<Pod> podInformer;
  private Lister<Pod> podLister;

  private SparkSystemdStateMachine systemdStateMachine;
  private SparkClusterStateMachine clusterStateMachine;

  public SparkClusterController(KubernetesClient client, String crdPath, Long resyncCycle) {
    super(client, crdPath, resyncCycle);

    this.podInformer = informerFactory.sharedIndexInformerFor(Pod.class, PodList.class, resyncCycle);
    this.podLister = new Lister<>(podInformer.getIndexer(), namespace);

    this.systemdStateMachine = new SparkSystemdStateMachine(this);
    this.clusterStateMachine = new SparkClusterStateMachine(this);
    // register pods
    registerPodEventHandler();
  }

  @Override
  protected void waitForAllInformersSynced() {
    while (!crdSharedIndexInformer.hasSynced() || !podInformer.hasSynced()) {
      ;
    }
    logger.info("SparkCluster informer and pod informer initialized ... waiting for changes");
  }

  /**
   * Register event handler for kubernetes pods
   */
  private void registerPodEventHandler() {
    podInformer.addEventHandler(new ResourceEventHandler<Pod>() {
      @Override
      public void onAdd(Pod pod) {
        logger.trace("onAddPod: " + pod);
        handlePodObject(pod);
      }

      @Override
      public void onUpdate(Pod oldPod, Pod newPod) {
        if (oldPod.getMetadata().getResourceVersion().equals(newPod.getMetadata().getResourceVersion())) {
          return;
        }
        logger.trace("onUpdate:\npodOld: " + oldPod + "\npodNew: " + newPod);
        handlePodObject(newPod);
      }

      @Override
      public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
        logger.trace("onDeletePod: " + pod);
      }
    });
  }

  @Override
  public void process(SparkCluster cluster) {
    // TODO: except stopped?
    // only go for cluster state machine if no systemd action is currently running
    if (systemdStateMachine.process(cluster) == true) {
      return;
    }
    clusterStateMachine.process(cluster);
  }

  /**
   * Create pods with regard to spec and current state
   *
   * @param pods    - list of available pods belonging to the given node
   * @param cluster - current spark cluster
   * @param node    - master or worker node
   *
   * @return list of created pods
   */
  public List<Pod> createPods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    List<Pod> createdPods = new ArrayList<Pod>();
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
            .create(createNewPod(cluster, node, selector));
          createdPods.add(pod);
        }
      }
    }
    return createdPods;
  }

  /**
   * Create pod for spark node
   *
   * @param cluster - spark cluster
   * @param node    - master or worker node
   *
   * @return pod create from specs
   */
  private Pod createNewPod(SparkCluster cluster, SparkNode node, SparkNodeSelector selector) {
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
      .withTolerations(node.getTolerations())
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
      .withMountPath(SparkOperatorConfig.POD_CONTAINER_VOLUME_MOUNT_PATH.toString())
      .withName(cmName)
      .endVolumeMount()
      .withEnv(List.copyOf(node.getEnv()))
      .endContainer()
      .endSpec()
      .build();
  }

  /**
   * Split existing pods according to spec in selectors and their hostnames
   *
   * @param pods - list of existing pods
   * @param node - spark node (master/worker)
   *
   * @return HashMap<String, List < Pod> where the key is the hostname and value is a list of pods with that hostname
   */
  private Map<SparkNodeSelector, List<Pod>> splitPodsBySelector(List<Pod> pods, SparkNode node) {
    Map<SparkNodeSelector, List<Pod>> podsByHost = new HashMap<>();

    // check in each node selector
    for (SparkNodeSelector selector : node.getSelectors()) {
      // check if pod.nodename equals selector.matchlabels.hostname TODO: remove hardcoded
      String hostName = selector.getMatchLabels().get(SparkOperatorConfig.KUBERNETES_IO_HOSTNAME.toString());

      if (hostName != null) {
        podsByHost.put(selector, new ArrayList<Pod>());
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
   * @param pod  - pod with node / hostname
   * @param node - spark master / worker node
   *
   * @return selector corresponding to pod
   */
  private Map<Pod, SparkNodeSelector> getSelectorsForPod(List<Pod> pods, SparkNode node) {
    Map<Pod, SparkNodeSelector> matchPodOnSelector = new HashMap<>();
    // for each selector
    for (SparkNodeSelector selector : node.getSelectors()) {
      int instances = selector.getInstances();
      // for each pod check if matchlabels is a fit and
      for (Pod pod : pods) {
        // if hostnames match && instances left from spec && pod no selector yet
        if (pod.getSpec().getNodeName().equals(
          selector.getMatchLabels().get(SparkOperatorConfig.KUBERNETES_IO_HOSTNAME.toString()))
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
   * Delete pods with regard to spec and current state -> for reconcilation
   *
   * @param pods    - list of available pods belonging to the given node
   * @param cluster - current spark cluster
   * @param node    - master or worker node
   *
   * @return list of deleted pods
   */
  public List<Pod> deletePods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    List<Pod> deletedPods = new ArrayList<Pod>();
    // remember processed pods to delete pods not machting any selector
    List<Pod> processedPods = new ArrayList<Pod>();

    Map<SparkNodeSelector, List<Pod>> podsBySelector = splitPodsBySelector(pods, node);
    // delete pods from selector if not matching spec
    for (Entry<SparkNodeSelector, List<Pod>> entry : podsBySelector.entrySet()) {
      List<Pod> podsInSelector = entry.getValue();
      SparkNodeSelector selector = entry.getKey();
      // add pods to processedPods
      processedPods.addAll(podsInSelector);
      // If more pods than spec delete old pods
      int diff = podsInSelector.size() - selector.getInstances();

      for (; diff > 0; diff--) {
        // TODO: dont remove current master leader!
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

    logger.debug(String.format("[%s] - deleted %d %s pod(s): %s",
      clusterStateMachine.getState().name(), deletedPods.size(), node.getPodTypeName(), metadataListToDebug(deletedPods)));
    return deletedPods;
  }

  /**
   * Delete all node (master/worker) pods in cluster with no regard to spec -> systemd
   *
   * @param cluster - cluster specification to retrieve all used pods
   *
   * @return list of deleted pods
   */
  public List<Pod> deletePods(SparkCluster cluster, SparkNode... nodes) {
    List<Pod> deletedPods = new ArrayList<Pod>();

    // if nodes are null take all
    if (nodes == null || nodes.length == 0) {
      nodes = new SparkNode[] {cluster.getSpec().getMaster(), cluster.getSpec().getWorker()};
    }

    // collect master and worker nodes
    List<Pod> pods = new ArrayList<Pod>();
    for (SparkNode node : nodes) {
      pods.addAll(getPodsByNode(cluster, node));
    }
    // delete pods
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
   * @param cluster - current spark cluster
   * @param nodes   - master or worker node
   *
   * @return list of pods belonging to the given node
   */
  public List<Pod> getPodsByNode(SparkCluster cluster, SparkNode... nodes) {
    List<Pod> podList = new ArrayList<>();

    // if nodes are null take all
    if (nodes == null || nodes.length == 0) {
      nodes = new SparkNode[] {cluster.getSpec().getMaster(), cluster.getSpec().getWorker()};
    }

    for (SparkNode node : nodes) {
      String nodeName = createPodName(cluster, node, false);

      List<Pod> pods = podLister.list();
      // not in cache (for testing)
      if (pods.size() == 0) {
        pods = client.pods().list().getItems();
      }

      for (Pod pod : pods) {
        // filter for pods not belonging to cluster
        if (podInCluster(pod) == null) {
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
   * Create pod name schema
   *
   * @param cluster  - current spark cluster
   * @param node     - master or worker node
   * @param withUUID - if true append generated UUID, if false keep generated name (cluster.name + "-" + node.podtypename)
   *
   * @return pod name
   */
  private String createPodName(SparkCluster cluster, SparkNode node, boolean withUUID) {
    String podName = cluster.getMetadata().getName() + "-" + node.getPodTypeName() + "-";
    if (withUUID) {
      podName += UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
    return podName;
  }

  /**
   * Create config map name for pod
   *
   * @param pod - pod with config map
   *
   * @return config map name
   */
  private String createConfigMapName(Pod pod) {
    return pod.getMetadata().getName() + "-cm";
  }

  /**
   * Check incoming pods for owner reference of SparkCluster and add to blocking queue
   *
   * @param pod - kubernetes pod
   */
  private void handlePodObject(Pod pod) {
    SparkCluster cluster = podInCluster(pod);
    if (cluster != null) {
      enqueueCrd(cluster);
    }
  }

  /**
   * Return the owner reference of that specific pod if available
   *
   * @param pod - fabric8 Pod
   *
   * @return pod owner reference
   */
  private OwnerReference getControllerOf(Pod pod) {
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
   * @param pod - pod to be checked for owner reference
   *
   * @return SparkCluster the pod belongs to, otherwise null
   */
  private SparkCluster podInCluster(Pod pod) {
    SparkCluster cluster = null;
    OwnerReference ownerReference = getControllerOf(pod);
    if (ownerReference == null) {
      return null;
    }
    // check if pod belongs to spark cluster
    String sparkClusterKind = getCrdContext(crdMetadata).getKind();
    if (ownerReference.getKind().equalsIgnoreCase(sparkClusterKind)) {
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
   * Create config map content for master / worker
   *
   * @param pods    - pods to create config maps for
   * @param cluster - spark cluster
   * @param node    - spark master / worker
   */
  public List<ConfigMap> createConfigMaps(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    List<ConfigMap> createdConfigMaps = new ArrayList<>();
    // match selector
    Map<Pod, SparkNodeSelector> matchPodToSelectors = getSelectorsForPod(pods, node);

    for (Entry<Pod, SparkNodeSelector> entry : matchPodToSelectors.entrySet()) {
      String cmName = createConfigMapName(entry.getKey());

      Resource<ConfigMap, DoneableConfigMap> configMapResource = client
        .configMaps()
        .inNamespace(cluster.getMetadata().getNamespace())
        .withName(cmName);
      //
      // create entry for spark-env.sh
      //
      StringBuffer sbEnv = new StringBuffer();
      // only worker has required information to be set
      Map<String, String> cmFiles = new HashMap<String, String>();
      // all known data in yaml and pojo for worker
      if (node.getPodTypeName().equals(SparkNodeWorker.POD_TYPE)) {
        addToSparkEnv(sbEnv, SparkConfig.SPARK_WORKER_CORES, entry.getValue().getCores());
        addToSparkEnv(sbEnv, SparkConfig.SPARK_WORKER_MEMORY, entry.getValue().getMemory());
      }

      cmFiles.put("spark-env.sh", sbEnv.toString());
      //
      // create entry for spark-defaults.conf
      //
      StringBuffer sbConf = new StringBuffer();
      // add secret
      String secret = cluster.getSpec().getSecret();
      if (secret != null && !secret.isEmpty()) {
        node.getSparkConfiguration().add(new EnvVar(SparkConfig.SPARK_AUTHENTICATE.getConfig(), "true", null));
        node.getSparkConfiguration().add(new EnvVar(SparkConfig.SPARK_AUTHENTICATE_SECRET.getConfig(), secret, null));
      }
      addToSparkConfig(node.getSparkConfiguration(), sbConf);

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

  /**
   * Get config map content for master / worker
   *
   * @param cluster - spark cluster
   * @param node    - spark master / worker
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
   * Delete config map content for master / worker
   *
   * @param cluster - spark cluster
   * @param node    - spark master / worker
   */
  public void deleteConfigMaps(List<Pod> pods, SparkCluster cluster, SparkNode node) {
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
   * @param cluster - spark cluster
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

    SparkNode[] nodes = new SparkNode[] {cluster.getSpec().getMaster(), cluster.getSpec().getWorker()};

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
   * Add spark environment variables to stringbuffer for spark-env.sh configuration
   *
   * @param sb     - string buffer to add to
   * @param config - key
   * @param value  - value
   */
  private void addToSparkEnv(StringBuffer sb, SparkConfig config, String value) {
    if (value != null && !value.isEmpty()) {
      sb.append(config.getEnv() + "=" + value + "\n");
    }
  }

  /**
   * Add to string buffer for spark configuration (spark-default.conf) in config map
   *
   * @param sparkConfiguration - spark config given in specs
   * @param sb                 - string buffer to add to
   */
  private void addToSparkConfig(Set<EnvVar> sparkConfiguration, StringBuffer sb) {
    for (EnvVar var : sparkConfiguration) {
      String name = var.getName();
      String value = var.getValue();
      if (name != null && !name.isEmpty() && value != null && !value.isEmpty()) {
        sb.append(name + " " + value + "\n");
      }
    }
  }

  /**
   * Check if all given pods have status "Running"
   *
   * @param pods   - list of pods
   * @param status - PodStatus to compare to
   *
   * @return true if all pods have status from Podstatus
   */
  public boolean allPodsHaveStatus(List<Pod> pods, PodState status) {
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
  public int getPodSpecToClusterDifference(SparkNode node, List<Pod> pods) {
    return node.getInstances() - pods.size();
  }

  /**
   * Extract leader node name from pods
   *
   * @param pods - list of master pods
   *
   * @return null or pod.spec.nodeName if available
   */
  public List<String> getHostNames(List<Pod> pods) {
    // TODO: determine master leader
    List<String> nodeNames = new ArrayList<String>();
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
   * @param cluster         - spark cluster
   * @param masterNodeNames - list of available master nodes
   */
  public String adaptWorkerCommand(SparkCluster cluster, List<String> masterNodeNames) {
    String masterUrl = null;
    // adapt command in workers
    List<String> commands = cluster.getSpec().getWorker().getCommands();

    if (commands.size() == 1) {
      String port = "7077";
      // if different port in env
      for (EnvVar var : cluster.getSpec().getWorker().getEnv()) {
        if (var.getName().equals("SPARK_MASTER_PORT")) {
          port = var.getValue();
        }
      }

      StringBuffer sb = new StringBuffer();
      sb.append("spark://");
      for (int i = 0; i < masterNodeNames.size(); i++) {
        sb.append(masterNodeNames.get(i) + ":" + port);
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
   * @param pods - list of pods
   *
   * @return pod metadata.name
   */
  public List<String> metadataListToDebug(List<? extends HasMetadata> hasMetadata) {
    List<String> output = new ArrayList<String>();
    hasMetadata.forEach((n) -> output.add(n.getMetadata().getName()));
    return output;
  }

  public SparkSystemdStateMachine getSystemdStateMachine() {
    return systemdStateMachine;
  }

  public SparkClusterStateMachine getClusterStateMachine() {
    return clusterStateMachine;
  }

}
