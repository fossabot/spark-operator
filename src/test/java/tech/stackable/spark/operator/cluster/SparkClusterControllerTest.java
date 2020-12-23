package tech.stackable.spark.operator.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatus;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusCommand;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatusSystemd;
import tech.stackable.spark.operator.cluster.crd.SparkNode;
import tech.stackable.spark.operator.common.type.SparkOperatorConfig;

@TestInstance(Lifecycle.PER_CLASS)
public class SparkClusterControllerTest {

  public KubernetesServer server;
  public KubernetesClient client;

  public SparkClusterController controller;

  public String crdPath = "cluster/spark-cluster-crd.yaml";
  public String crdExamplePath = "cluster/spark-cluster-example.yaml";

  public long resyncCycle = 5 * 1000L;

  public static String SPARK_CLUSTER_KIND = "SparkCluster";

  @BeforeEach
  public void init() throws UnknownHostException {
    server = new KubernetesServer(true, true);
    server.before();
    client = server.getClient();
    controller = new SparkClusterController(client, crdPath, resyncCycle);
  }

  @AfterEach
  public void cleanUp() {
    client.close();
    server.after();
  }

  @Test
  public void testLoadYaml() {
    List<HasMetadata> crdList = controller.loadYaml(crdPath);
    assertNotNull(crdList);
    assertEquals(1, crdList.size());

    CustomResourceDefinition crd = (CustomResourceDefinition) crdList.get(0);
    assertEquals(SPARK_CLUSTER_KIND, crd.getSpec().getNames().getKind());
  }

  @Test
  public void testCrdClientCrud() throws IOException {
    // load spark-cluster-example.yaml
    SparkCluster cluster = loadClusterExample();
    assertNotNull(cluster);
    assertEquals(SPARK_CLUSTER_KIND, cluster.getKind());

    // list all sparkclusters -> only one loaded
    List<SparkCluster> clusters = controller.getCrdClient().list().getItems();
    assertNotNull(clusters);
    assertEquals(1, clusters.size());

    // create status command
    String testCommand = "test";
    SparkClusterStatus clusterStatus =
      new SparkClusterStatus.Builder()
        .withSystemdStatus(
          new SparkClusterStatusSystemd.Builder()
            .withRunningCommands(
              new SparkClusterStatusCommand.Builder()
                .withCommand(testCommand)
                .build())
            .build())
        .build();

    cluster.setStatus(clusterStatus);

    // update status
    SparkCluster clusterWithStatus = controller.getCrdClient().updateStatus(cluster);
    assertNotNull(clusterWithStatus);

    // get all spark clusters -> only one created and check for status
    clusters = controller.getCrdClient().list().getItems();
    assertNotNull(clusters);
    assertEquals(1, clusters.size());
    assertEquals(testCommand, clusters.get(0).getStatus().getSystemd().getRunningCommand().getCommand());

    // delete
    controller.getCrdClient().delete(clusters.get(0));
    // get all spark clusters -> only one created and check for status
    clusters = controller.getCrdClient().list().getItems();
    assertNotNull(clusters);
    assertEquals(0, clusters.size());
  }

  @Test
  public void testPodCrud() {
    // load spark-cluster-example.yaml
    SparkCluster cluster = loadClusterExample();
    assertNotNull(cluster);
    assertEquals(SPARK_CLUSTER_KIND, cluster.getKind());
    // get node instances
    Integer masterPodInstances = cluster.getSpec().getMaster().getInstances();
    Integer workerPodInstances = cluster.getSpec().getWorker().getInstances();
    // test create master pods
    List<Pod> createdMasterPods = controller.createPods(new ArrayList<Pod>(), cluster, cluster.getSpec().getMaster());
    assertNotNull(createdMasterPods);
    assertEquals(masterPodInstances, createdMasterPods.size());
    // test create worker pods
    List<Pod> createdWorkerPods = controller.createPods(new ArrayList<Pod>(), cluster, cluster.getSpec().getWorker());
    assertNotNull(createdWorkerPods);
    assertEquals(workerPodInstances, createdWorkerPods.size());
    // test get master pods by node
    List<Pod> masterPods = controller.getPodsByNode(cluster, cluster.getSpec().getMaster());
    assertNotNull(masterPods);
    assertEquals(masterPodInstances, masterPods.size());
    // test get worker pods by node
    List<Pod> workerPods = controller.getPodsByNode(cluster, cluster.getSpec().getWorker());
    assertNotNull(workerPods);
    assertEquals(workerPodInstances, workerPods.size());
    // test get all nodes
    List<Pod> allPods = controller.getPodsByNode(cluster);
    assertNotNull(allPods);
    assertEquals(masterPodInstances + workerPodInstances, allPods.size());
  }

  @Test
  public void testConfigMapsCrud() {
    SparkCluster cluster = loadClusterExample();
    assertNotNull(cluster);
    assertEquals(SPARK_CLUSTER_KIND, cluster.getKind());
    // get node instances
    Integer masterPodInstances = cluster.getSpec().getMaster().getInstances();
    Integer workerPodInstances = cluster.getSpec().getWorker().getInstances();
    // test create master pods
    List<Pod> createdMasterPods = controller.createPods(new ArrayList<Pod>(), cluster, cluster.getSpec().getMaster());
    assertNotNull(createdMasterPods);
    assertEquals(masterPodInstances, createdMasterPods.size());
    // test create worker pods
    List<Pod> createdWorkerPods = controller.createPods(new ArrayList<Pod>(), cluster, cluster.getSpec().getWorker());
    assertNotNull(createdWorkerPods);
    assertEquals(workerPodInstances, createdWorkerPods.size());

    // set node names
    setNodeNames(createdMasterPods, cluster.getSpec().getMaster());
    setNodeNames(createdWorkerPods, cluster.getSpec().getWorker());

    // create master config maps
    List<ConfigMap> createdMasterConfigMaps = controller.createConfigMaps(createdMasterPods, cluster, cluster.getSpec().getMaster());
    assertNotNull(createdMasterConfigMaps);
    assertEquals(masterPodInstances, createdMasterConfigMaps.size());
    // create worker config maps
    List<ConfigMap> createdWorkerConfigMaps = controller.createConfigMaps(createdWorkerPods, cluster, cluster.getSpec().getWorker());
    assertNotNull(createdWorkerConfigMaps);
    assertEquals(workerPodInstances, createdWorkerConfigMaps.size());

    // retrieve config maps
    List<ConfigMap> retrievedMasterConfigMaps = controller.getConfigMaps(createdMasterPods, cluster);
    assertNotNull(retrievedMasterConfigMaps);
    assertEquals(masterPodInstances, retrievedMasterConfigMaps.size());
    assertEquals(createdMasterConfigMaps, retrievedMasterConfigMaps);
    List<ConfigMap> retrievedWorkerConfigMaps = controller.getConfigMaps(createdWorkerPods, cluster);
    assertNotNull(retrievedWorkerConfigMaps);
    assertEquals(workerPodInstances, retrievedWorkerConfigMaps.size());
    // compare via sets to ignore order
    assertEquals(new HashSet<ConfigMap>(createdWorkerConfigMaps),
      new HashSet<ConfigMap>(retrievedWorkerConfigMaps));
    // delete master config maps
    controller.deleteConfigMaps(createdMasterPods, cluster, cluster.getSpec().getMaster());
    retrievedMasterConfigMaps = controller.getConfigMaps(createdMasterPods, cluster);
    assertNotNull(retrievedMasterConfigMaps);
    assertEquals(0, retrievedMasterConfigMaps.size());
    // delete all config maps
    controller.deleteAllClusterConfigMaps(cluster);
    // master config maps deleted
    retrievedMasterConfigMaps = controller.getConfigMaps(createdMasterPods, cluster);
    assertNotNull(retrievedMasterConfigMaps);
    assertEquals(0, retrievedMasterConfigMaps.size());
    // worker config maps deleted
    retrievedWorkerConfigMaps = controller.getConfigMaps(createdMasterPods, cluster);
    assertNotNull(retrievedWorkerConfigMaps);
    assertEquals(0, retrievedWorkerConfigMaps.size());
  }

  private SparkCluster loadClusterExample() {
    // load spark-cluster-example.yaml
    SparkCluster cluster =
      controller.getCrdClient()
        .load(Thread.currentThread().getContextClassLoader().getResourceAsStream(crdExamplePath))
        .create();
    cluster.getMetadata().setUid("123456789");
    cluster.getMetadata().setNamespace(client.getNamespace());

    return cluster;
  }

  private void setNodeNames(List<Pod> pods, SparkNode node) {
    if (node.getSelectors() != null && node.getSelectors().size() > 0) {
      String nodeName = node.getSelectors().get(0).getMatchLabels().get(SparkOperatorConfig.KUBERNETES_IO_HOSTNAME.toString());
      for (Pod pod : pods) {
        pod.getSpec().setNodeName(nodeName);
      }
    }
  }

}
