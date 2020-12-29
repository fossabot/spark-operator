package tech.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.Toleration;

@JsonDeserialize
public class SparkNode implements KubernetesResource {

  private static final long serialVersionUID = 5917995090358580518L;

  @JsonIgnore
  private String podTypeName;

  private List<SparkNodeSelector> selectors = new ArrayList<>();
  private List<Toleration> tolerations = new ArrayList<>();
  private List<String> commands = new ArrayList<>();
  private List<String> args = new ArrayList<>();
  private Set<EnvVar> sparkConfiguration = new HashSet<>();
  private Set<EnvVar> env = new HashSet<>();

  public SparkNode() {
  }

  public SparkNode(
    List<SparkNodeSelector> selectors, List<Toleration> tolerations, List<String> commands,
    List<String> args, Set<EnvVar> sparkConfiguration, Set<EnvVar> env) {
    this.selectors = selectors;
    this.tolerations = tolerations;
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

  public String getPodTypeName() {
    return podTypeName;
  }

  protected final void setTypeName(String typeName) {
    podTypeName = typeName;
  }

  public List<SparkNodeSelector> getSelectors() {
    return selectors;
  }

  public void setSelectors(List<SparkNodeSelector> selectors) {
    this.selectors = selectors;
  }

  public List<Toleration> getTolerations() {
    return tolerations;
  }

  public void setTolerations(List<Toleration> tolerations) {
    this.tolerations = tolerations;
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
    private List<Toleration> tolerations = new ArrayList<>();
    private List<String> commands = new ArrayList<>();
    private List<String> args = new ArrayList<>();
    private Set<EnvVar> sparkConfiguration = new HashSet<>();
    private Set<EnvVar> env = new HashSet<>();

    public Builder withSparkSelectors(List<SparkNodeSelector> selectors) {
      this.selectors = selectors;
      return this;
    }

    public Builder withTolerations(List<Toleration> tolerations) {
      this.tolerations = tolerations;
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
      return new SparkNode(selectors, tolerations, commands, args, sparkConfiguration, env);
    }

  }
}
