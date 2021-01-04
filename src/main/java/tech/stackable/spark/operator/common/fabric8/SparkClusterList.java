package tech.stackable.spark.operator.common.fabric8;

import io.fabric8.kubernetes.client.CustomResourceList;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;

public class SparkClusterList extends CustomResourceList<SparkCluster> {

  private static final long serialVersionUID = -8453549963001365047L;
}
