package tech.stackable.spark.operator.common.fabric8;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;

public class SparkClusterDoneable extends CustomResourceDoneable<SparkCluster> {

  public SparkClusterDoneable(SparkCluster cluster, Function<SparkCluster, SparkCluster> function) {
    super(cluster, function);
  }
}
