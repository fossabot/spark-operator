package com.stackable.spark.operator.common.type;

public enum SparkClusterState {
	/**
	 * INITIAL:
	 * Initialize and clear resources
	 */
    INITIAL("INITIAL"),
	/**
	 * CREATE_SPARK_MASTER:
	 * Spawn master pods given by specification
	 */
    CREATE_SPARK_MASTER("CREATE_SPARK_MASTER"),
    /**
     * WAIT_FOR_MASTER_HOST_NAME: 
     * after master is created, wait for agent to set the dedicated host name and use for workers 
     * ==> create config map
     */
    WAIT_FOR_MASTER_HOST_NAME("WAIT_FOR_MASTER_HOST_NAME"),
    /**
     * WAIT_FOR_MASTER_RUNNING
     * Wait before the master is configured and running before spawning workers
     */
    WAIT_FOR_MASTER_RUNNING("WAIT_FOR_MASTER_RUNNING"),
    /**
     * CREATE_SPARK_WORKER:
     * Spawn worker pods given by specification
     */
    CREATE_SPARK_WORKER("CREATE_SPARK_WORKER") ,
    /**
     * WAIT_FOR_WORKERS_RUNNING
     * Wait for all workers to run
     */
    WAIT_FOR_WORKERS_RUNNING("WAIT_FOR_WORKERS_RUNNING"),
    /**
     * RECONCILE:
     * Watch the cluster state and act if spec != state
     */
    RECONCILE("RECONCILE");
    
	private String state;
	
    SparkClusterState(String state) {
		this.state = state;
	}
    
    public String toString() {
    	return state;
    }
}