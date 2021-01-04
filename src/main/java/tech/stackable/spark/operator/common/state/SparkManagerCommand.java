package tech.stackable.spark.operator.common.state;

public enum SparkManagerCommand {
  UNKNOWN("UNKNOWN"),
  START("START"),
  STOP("STOP"),
  RESTART("RESTART"),
  UPDATE("UPDATE");

  private final String state;

  SparkManagerCommand(String state) {
    this.state = state;
  }

  public String toString() {
    return state;
  }

  public static SparkManagerCommand getManagerCommand(String command) {
    if (command != null) {
      SparkManagerCommand[] commands = {START, STOP, UPDATE, RESTART};

      for (SparkManagerCommand managerCommand : commands) {
        if (managerCommand.toString().equals(command.toUpperCase())) {
          return managerCommand;
        }
      }
    }
    return UNKNOWN;
  }
}
