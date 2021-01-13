package tech.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize
public class SparkNode implements KubernetesResource {

  private static final long serialVersionUID = 5917995090358580518L;

  @JsonIgnore
  private SparkNodeType nodeType;

  private List<SparkNodeSelector> selectors = new ArrayList<>();
  private List<String> commands = new ArrayList<>();
  private List<String> args = new ArrayList<>();
  private Set<EnvVar> sparkConfiguration = new HashSet<>();
  private Set<EnvVar> env = new HashSet<>();

  public SparkNode() {}

  public SparkNode(
    List<SparkNodeSelector> selectors, List<String> commands,
    List<String> args, Set<EnvVar> sparkConfiguration, Set<EnvVar> env) {

    this.selectors = selectors;
    this.commands = commands;
    this.args = args;
    this.sparkConfiguration = sparkConfiguration;
    this.env = env;
  }

  /**
   * Add all instances in given selectors
   *
   * @return sum of instances in given selectors
   */
  @JsonIgnore
  public Integer getInstances() {
    int instances = 0;
    if (selectors.size() != 0) {
      for (SparkNodeSelector selector : selectors) {
        instances += selector.getInstances();
      }
    }
    return instances;
  }

  /**
   * Return all available values given by a label for selector (e.g. host name)
   *
   * @param selectorLabel key in matchlabels to retrieve value from
   * @return set of found values
   */
  @JsonIgnore
  public Set<String> getSelectorLabels(String selectorLabel) {
    Set<String> labelValues = new HashSet<>();
    for (SparkNodeSelector selector : selectors) {
      if (selector.getMatchLabels() == null) {
        continue;
      }
      String value = selector.getMatchLabels().get(selectorLabel);
      if (value != null) {
        labelValues.add(value);
      }
    }
    return labelValues;
  }

  public SparkNodeType getNodeType() {
    return nodeType;
  }

  protected final void setTypeName(SparkNodeType typeName) {
    nodeType = typeName;
  }

  public List<SparkNodeSelector> getSelectors() {
    return selectors;
  }

  public void setSelectors(List<SparkNodeSelector> selectors) {
    this.selectors = selectors;
  }

  public List<String> getCommands() {
    return commands;
  }

  public void setCommands(List<String> commands) {
    this.commands = commands;
  }

  public List<String> getCommandArgs() {
    return args;
  }

  public void setCommandArgs(List<String> commandArgs) {
    args = commandArgs;
  }

  public List<String> getArgs() {
    return args;
  }

  public void setArgs(List<String> args) {
    this.args = args;
  }

  public Set<EnvVar> getSparkConfiguration() {
    return sparkConfiguration;
  }

  public void setSparkConfiguration(Set<EnvVar> sparkConfiguration) {
    this.sparkConfiguration = sparkConfiguration;
  }

  public Set<EnvVar> getEnv() {
    return env;
  }

  public void setEnv(Set<EnvVar> env) {
    this.env = env;
  }

  public static class Builder {

    private List<SparkNodeSelector> selectors = new ArrayList<>();
    private List<String> commands = new ArrayList<>();
    private List<String> args = new ArrayList<>();
    private Set<EnvVar> sparkConfiguration = new HashSet<>();
    private Set<EnvVar> env = new HashSet<>();

    public Builder withSparkSelectors(List<SparkNodeSelector> selectors) {
      this.selectors = selectors;
      return this;
    }

    public Builder withCommands(List<String> commands) {
      this.commands = commands;
      return this;
    }

    public Builder withArgs(List<String> args) {
      this.args = args;
      return this;
    }

    public Builder withSparkConfiguration(Set<EnvVar> sparkConfiguration) {
      this.sparkConfiguration = sparkConfiguration;
      return this;
    }

    public Builder withEnvVars(Set<EnvVar> env) {
      this.env = env;
      return this;
    }

    public SparkNode build() {
      return new SparkNode(selectors, commands, args, sparkConfiguration, env);
    }

  }

  public enum SparkNodeType {
    MASTER("master"),
    WORKER("worker"),
    HISTORY_SERVER("historyserver");

    private final String type;

    SparkNodeType(String type) {
      this.type = type;
    }

    public String toString() {
      return type;
    }
  }

}
