package tech.stackable.spark.operator.cluster.statemachine;

public interface SparkStateMachine<CrdClass, Event> {

  /**
   * process state machine: get events and run transitions
   *
   * @param crd - crd class
   *
   * @return true if any transitions took place
   */
  boolean process(CrdClass crd);

  /**
   * extract an event from the crd state
   *
   * @param crd - spark cluster
   *
   * @return event
   */
  Event getEvent(CrdClass crd);

  /**
   * Apply transitions through the state machine depending on incoming events
   *
   * @param crd - crd class
   * @param event   - for the transition
   */
  void transition(CrdClass crd, Event event);
}
