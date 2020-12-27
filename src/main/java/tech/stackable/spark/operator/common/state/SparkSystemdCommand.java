package tech.stackable.spark.operator.common.state;

public enum SparkSystemdCommand {
  UNKNOWN("UNKNOWN"),
  START("START"),
  STOP("STOP"),
  RESTART("RESTART"),
  UPDATE("UPDATE");

  private final String state;

  SparkSystemdCommand(String state) {
    this.state = state;
  }

  public String toString() {
    return state;
  }

  public static SparkSystemdCommand getSystemdCommand(String command) {
    if (command != null) {
      SparkSystemdCommand[] commands = {START, STOP, UPDATE, RESTART};

      for (SparkSystemdCommand systemdCommand : commands) {
        if (systemdCommand.toString().equals(command.toUpperCase())) {
          return systemdCommand;
        }
      }
    }
    return UNKNOWN;
  }
}
