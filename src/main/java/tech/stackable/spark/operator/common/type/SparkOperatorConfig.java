package tech.stackable.spark.operator.common.type;

public enum SparkOperatorConfig {
	KUBERNETES_IO_HOSTNAME("kubernetes.io/hostname"),
	POD_SELECTOR_NAME("selectorName"),
	POD_CONTAINER_VOLUME_MOUNT_PATH("conf");
	
	private String value;
	
	private SparkOperatorConfig(String value) {
		this.value = value;
	}
	
	public String toString() {
		return value;
	}
}
