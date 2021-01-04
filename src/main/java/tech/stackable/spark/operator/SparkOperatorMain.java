package tech.stackable.spark.operator;

import io.fabric8.kubernetes.client.KubernetesClientException;
import tech.stackable.spark.operator.application.SparkApplicationController;
import tech.stackable.spark.operator.cluster.SparkClusterController;
import tech.stackable.spark.operator.cluster.manager.SparkManagerController;

/**
 * Main Class for Spark Operator: run via this command:
 *
 * mvn exec:java -Dexec.mainClass=com.stackable.spark.operator.SparkOperatorMain
 */
public class SparkOperatorMain {

  // 300 seconds = 5 minutes
  private static final long RESYNC_CYCLE = 300 * 1000L;

  public static void main(String... args) {
    String clusterCrdPath = "cluster/spark-cluster-crd.yaml";
    String managerCrdPath = "manager/spark-manager-crd.yaml";
    String applicationCrdPath = "application/spark-application-crd.yaml";

    try {
      SparkClusterController sparkClusterController = new SparkClusterController(
        null,
        clusterCrdPath,
        RESYNC_CYCLE
      );

      SparkManagerController sparkSystemdController = new SparkManagerController(
        null,
        managerCrdPath,
        clusterCrdPath,
        RESYNC_CYCLE
      );

      SparkApplicationController sparkApplicationController = new SparkApplicationController(
        null,
        applicationCrdPath,
        RESYNC_CYCLE
      );

      // start different controllers
      Thread sparkClusterControllerThread = new Thread(sparkClusterController);
      sparkClusterControllerThread.start();

      Thread sparkSystemdThread = new Thread(sparkSystemdController);
      sparkSystemdThread.start();

      Thread sparkApplicationThread = new Thread(sparkApplicationController);
      sparkApplicationThread.start();
    }
    catch(KubernetesClientException clientException) {
      System.out.println("No API server reachable - Operator exited! " + clientException.getMessage());
    }
  }

}
