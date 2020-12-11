package com.stackable.spark.operator.common.state;

public enum SparkSystemdState {
	SYSTEMD_INITIAL("SYSTEMD_INITIAL"),
	SYSTEMD_RESTART("SYSTEMD_RESTART"),
	SYSTEMD_UPDATE("SYSTEMD_UPDATE"),
	SYSTEMD_WAIT_FOR_PODS_DELETED("SYSTEMD_WAIT_FOR_PODS_DELETED");

	private String state;
	
	SparkSystemdState(String state) {
		this.state = state;
	}
	
	public String toString() {
		return state;
	}
}
