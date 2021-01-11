package tech.stackable.spark.operator.cluster.versioned;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
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

public abstract class SparkVersionedClusterController {

  private final KubernetesClient client;
  private final Lister<Pod> podLister;
  private final Lister<SparkCluster> crdLister;
  private final MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> crdClient;

  protected SparkVersionedClusterController(KubernetesClient client, Lister<Pod> podLister, Lister<SparkCluster> crdLister,
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
   * @return pod created from specs
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
      Map<SparkNodeSelector, List<Pod>> podsByHostName = SparkVersionedClusterControllerHelper.splitPodsBySelector(pods, node);

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
        if (cm.getMetadata().getName().startsWith(SparkVersionedClusterControllerHelper.createPodName(cluster, node, false))) {
          client.configMaps().delete(cm);
          deletedConfigMaps.add(cm);
        }
      }
    }
    return deletedConfigMaps;
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
   * Delete config map content for pods
   *
   * @param cluster spark cluster
   */
  public void deleteConfigMaps(List<Pod> pods, SparkCluster cluster) {
    for (Pod pod : pods) {
      String cmName = SparkVersionedClusterControllerHelper.createConfigMapName(pod);

      Resource<ConfigMap, DoneableConfigMap> configMapResource = client
          .configMaps()
          .inNamespace(cluster.getMetadata().getNamespace())
          .withName(cmName);

      // delete
      configMapResource.delete();
    }
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
  public List<Pod> deletePods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    List<Pod> deletedPods = new ArrayList<>();
    // remember processed pods to delete pods not matching any selector
    List<Pod> processedPods = new ArrayList<>();

    Map<SparkNodeSelector, List<Pod>> podsBySelector = SparkVersionedClusterControllerHelper.splitPodsBySelector(pods, node);
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
   * Return utilized implementation of kubernetes client
   * @return kubernetes client
   */
  public KubernetesClient getClient() {
    return client;
  }

  /**
   * SparkCluster mixed operation for CRUD operations
   * @return mixed operation for spark cluster
   */
  public MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> getCrdClient() {
    return crdClient;
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
      String nodeName = SparkVersionedClusterControllerHelper.createPodName(cluster, node, false);

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
      String cmName = SparkVersionedClusterControllerHelper.createConfigMapName(pod);

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
   * Check if pod belongs to cluster
   *
   * @param pod pod to be checked for owner reference
   *
   * @return SparkCluster the pod belongs to, otherwise null
   */
  public SparkCluster podInCluster(Pod pod, String kind) {
    OwnerReference ownerReference = SparkVersionedClusterControllerHelper.getControllerOf(pod);
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

}
