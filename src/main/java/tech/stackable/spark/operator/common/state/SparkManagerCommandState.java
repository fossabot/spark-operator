package tech.stackable.spark.operator.common.state;

public enum SparkManagerCommandState {
  STARTED("STARTED"),
  RUNNING("RUNNING"),
  ABORTED("ABORTED"),
  FINISHED("FINISHED"),
  FAILED("FAILED");

  private final String state;

  SparkManagerCommandState(String state) {
    this.state = state;
  }

  public String toString() {
    return state;
  }
}
