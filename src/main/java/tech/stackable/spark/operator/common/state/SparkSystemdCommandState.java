package tech.stackable.spark.operator.common.state;

public enum SparkSystemdCommandState {
	STARTED("STARTED"),
	RUNNING("RUNNING"),
	ABORTED("ABORTED"),
	FINISHED("FINISHED"),
	FAILED("FAILED");

	private String state;
	
	SparkSystemdCommandState(String state) {
		this.state = state;
	}
	
	public String toString() {
		return state;
	}
}
