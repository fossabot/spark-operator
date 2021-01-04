package tech.stackable.spark.operator.cluster.crd;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize
public class SparkNodeSelector implements KubernetesResource {

  private static final long serialVersionUID = 2535064095918732663L;

  private String name;
  private String nodeName;
  private Integer instances = 1;
  private String memory;
  private String cores;
  private Map<String, String> matchLabels = new HashMap<>();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public Integer getInstances() {
    return instances;
  }

  public void setInstances(Integer instances) {
    this.instances = instances;
  }

  public String getMemory() {
    return memory;
  }

  public void setMemory(String memory) {
    this.memory = memory;
  }

  public String getCores() {
    return cores;
  }

  public void setCores(String cores) {
    this.cores = cores;
  }

  public Map<String, String> getMatchLabels() {
    return matchLabels;
  }

  public void setMatchLabels(Map<String, String> matchLabels) {
    this.matchLabels = matchLabels;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SparkNodeSelector that = (SparkNodeSelector) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(nodeName, that.nodeName) &&
      Objects.equals(instances, that.instances) &&
      Objects.equals(memory, that.memory) &&
      Objects.equals(cores, that.cores) &&
      Objects.equals(matchLabels, that.matchLabels);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, nodeName, instances, memory, cores, matchLabels);
  }
}
