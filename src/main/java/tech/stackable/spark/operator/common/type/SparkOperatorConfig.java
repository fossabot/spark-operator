package tech.stackable.spark.operator.common.type;

public enum SparkOperatorConfig {
  KUBERNETES_IO_HOSTNAME("kubernetes.io/hostname"),

  POD_SELECTOR_NAME("selectorName"),
  POD_CONF_VOLUME_MOUNT_PATH("conf"),
  POD_LOG_VOLUME_MOUNT_PATH("spark-events");

  private final String value;

  SparkOperatorConfig(String value) {
    this.value = value;
  }

  public String toString() {
    return value;
  }
}
