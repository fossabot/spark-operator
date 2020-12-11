package com.stackable.spark.operator.common.state;

public enum SparkSystemdActionState {
	RUNNING("RUNNING"),
	ABORTED("ABORTED"),
	FINISHED("FINISHED"),
	FAILED("FAILED");

	private String state;
	
	SparkSystemdActionState(String state) {
		this.state = state;
	}
	
	public String toString() {
		return state;
	}
}
