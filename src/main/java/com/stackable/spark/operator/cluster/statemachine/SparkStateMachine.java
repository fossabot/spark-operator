package com.stackable.spark.operator.cluster.statemachine;

public interface SparkStateMachine<CrdClass,Event> {
	/**
	 * Process cluster in the state machine: get events and run transitions 
	 * @param cluster - spark cluster
	 * @return true if any transitions took place
	 */
	public boolean process(CrdClass crd);
	/**
	 * Extract an event from the cluster state
	 * @param cluster - spark cluster
	 * @return event
	 */
	public Event getEvent(CrdClass crd);
	/**
	 * Apply transitions through the state machine depending on incoming events
	 * @param cluster - spark cluster
	 * @param event - for the transition 
	 */
	public void transition(CrdClass crd, Event event);
}
