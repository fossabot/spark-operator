package com.stackable.spark.operator.common.state;

public enum SparkSystemdState {
	SYSTEMD_INITIAL("SYSTEMD_INITIAL"),
	SYSTEMD_UPDATE("SYSTEMD_UPDATE"),
	SYSTEMD_WAIT_FOR_IMAGE_UPDATED("SYSTEMD_WAIT_FOR_IMAGE_UPDATED"),
	SYSTEMD_RESTART("SYSTEMD_RESTART"),
	SYSTEMD_WAIT_FOR_JOBS_FINISHED("SYSTEMD_WAIT_FOR_JOBS_FINISHED"),
	SYSTEMD_WAIT_FOR_PODS_DELETED("SYSTEMD_WAIT_FOR_PODS_DELETED");

	private String state;
	
	SparkSystemdState(String state) {
		this.state = state;
	}
	
	public String toString() {
		return state;
	}
}
