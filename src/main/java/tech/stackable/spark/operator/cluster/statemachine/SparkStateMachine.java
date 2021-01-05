package tech.stackable.spark.operator.cluster.statemachine;

import tech.stackable.spark.operator.cluster.versioned.SparkVersionedClusterController;

public interface SparkStateMachine<CrdClass, Event, State> {

  /**
   * process state machine: get events and run transitions
   *
   * @param crd crd class
   *
   * @return true if any transitions took place
   */
  boolean process(CrdClass crd);

  /**
   * extract an event from the crd state
   *
   * @param crd spark cluster
   *
   * @return event
   */
  Event transition(CrdClass crd);

  /**
   * Apply transitions through the state machine depending on incoming events
   *
   * @param crd   crd class
   * @param event for the transition
   */
  void doAction(CrdClass crd, Event event);

  /**
   * Retrieve the current state of the state machine
   * @return current state of the state machine
   */
  State getState();

  /**
   * Set the versioned controller required for the spark cluster
   * @param controller
   */
  void setVersionedController(SparkVersionedClusterController controller);
}
