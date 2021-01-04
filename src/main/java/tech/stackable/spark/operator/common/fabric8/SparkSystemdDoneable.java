package tech.stackable.spark.operator.common.fabric8;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import tech.stackable.spark.operator.cluster.manager.crd.SparkManager;

public class SparkSystemdDoneable extends CustomResourceDoneable<SparkManager> {

  public SparkSystemdDoneable(SparkManager systemd, Function<SparkManager, SparkManager> function) {
    super(systemd, function);
  }
}

