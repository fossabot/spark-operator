package com.stackable.spark.operator.common.type;

public enum SparkSystemdAction {
	NONE("NONE"),
	STOP("STOP"),
	START("START"),
	RESTART("RESTART"),
	UPDATE("UPDATE");

	private String status;
	
	SparkSystemdAction(String status) {
		this.status = status;
	}
	
	public String toString() {
		return status;
	}
}
