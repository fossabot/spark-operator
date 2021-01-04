package tech.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize
@JsonInclude(Include.NON_NULL)
public class SparkClusterStatusManager implements KubernetesResource {

  private static final long serialVersionUID = 7259902757939136149L;

  private SparkClusterStatusCommand runningCommand;
  private List<String> stagedCommands;

  public SparkClusterStatusManager() {}

  public SparkClusterStatusManager(SparkClusterStatusCommand runningCommand, List<String> stagedCommands) {
    this.runningCommand = runningCommand;
    this.stagedCommands = stagedCommands;
  }

  public SparkClusterStatusCommand getRunningCommand() {
    return runningCommand;
  }

  public void setRunningCommand(SparkClusterStatusCommand runningCommand) {
    this.runningCommand = runningCommand;
  }

  public List<String> getStagedCommands() {
    return stagedCommands;
  }

  public void setStagedCommands(List<String> stagedCommands) {
    this.stagedCommands = stagedCommands;
  }

  public static class Builder {

    private SparkClusterStatusCommand runningCommand;
    private List<String> stagedCommands;

    public Builder withRunningCommands(SparkClusterStatusCommand runningCommand) {
      this.runningCommand = runningCommand;
      return this;
    }

    public Builder withStagedCommands(List<String> stagedCommands) {
      this.stagedCommands = stagedCommands;
      return this;
    }

    public Builder withSingleStagedCommand(String stagedCommand) {
      if (stagedCommands == null) {
        stagedCommands = new ArrayList<>();
      }
      stagedCommands.add(stagedCommand);
      return this;
    }

    public SparkClusterStatusManager build() {
      return new SparkClusterStatusManager(runningCommand, stagedCommands);
    }

  }
}
