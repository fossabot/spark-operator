package com.stackable.spark.operator.common.state;

public enum PodState {
	/** 
	 * PodPending means the pod has been accepted by the system, but one or more of the containers
	 * has not been started. This includes time before being bound to a node, as well as time spent
	 * pulling images onto the host. 
	 */
	PENDING("Pending"),
	/** 
	 * PodRunning means the pod has been bound to a node and all of the containers have been started.
	 * At least one container is still running or is in the process of being restarted.
	 */
	RUNNING("Running"),
	/**
	 * PodSucceeded means that all containers in the pod have voluntarily terminated
	 * with a container exit code of 0, and the system is not going to restart any of these containers.
	 */
	SUCCEEDED("Succeeded"),
	/**
	 * PodFailed means that all containers in the pod have terminated, and at least one container has
	 * terminated in a failure (exited with a non-zero exit code or was stopped by the system).
	 */
	FAILED("Failed"),
	/**
	 * PodUnknown means that for some reason the state of the pod could not be obtained, typically due
	 * to an error in communicating with the host of the pod.
	 */
	UNKNOWN("Unknown");

	private String state;
	
	PodState(String state) {
		this.state = state;
	}
	
	public String toString() {
		return state;
	}
}
