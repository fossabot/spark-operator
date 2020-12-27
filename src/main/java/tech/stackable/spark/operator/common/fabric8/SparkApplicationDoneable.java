package tech.stackable.spark.operator.common.fabric8;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import tech.stackable.spark.operator.application.SparkApplication;

public class SparkApplicationDoneable extends CustomResourceDoneable<SparkApplication> {

  public SparkApplicationDoneable(SparkApplication application, Function function) {
    super(application, function);
  }
}

