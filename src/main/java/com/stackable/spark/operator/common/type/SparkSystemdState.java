package com.stackable.spark.operator.common.type;

public enum SparkSystemdState {
	/**
	 * INITIAL:
	 * Initialize and clear resources
	 */
    INITIAL("INITIAL"),
    STAGE_COMMAND("STAGE_COMMAND"),
    WAIT_FOR_COMMAND_RUNNING("WAIT_FOR_COMMAND_RUNNING"),
    WAIT_FOR_COMMAND_FINISHED("WAIT_FOR_COMMAND_FINISHED"),
    REMOVE_SYSTEMD("REMOVE_SYSTEMD");
    
	private String state;
	
	SparkSystemdState(String state) {
		this.state = state;
	}
    
    public String toString() {
    	return state;
    }
}
