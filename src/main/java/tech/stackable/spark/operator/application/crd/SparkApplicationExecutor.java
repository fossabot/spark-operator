package tech.stackable.spark.operator.application.crd;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkApplicationExecutor implements KubernetesResource {

  private static final long serialVersionUID = 5905650793866264723L;

  private Integer instances;
  private String cores;
  private String coreLimit;
  private String memory;
  private String memoryOverhead;
  private String image;
  private Map<String, String> labels = new HashMap<>();

  public Integer getInstances() {
    return instances;
  }

  public void setInstances(Integer instances) {
    this.instances = instances;
  }

  public String getCores() {
    return cores;
  }

  public void setCores(String cores) {
    this.cores = cores;
  }

  public String getCoreLimit() {
    return coreLimit;
  }

  public void setCoreLimit(String coreLimit) {
    this.coreLimit = coreLimit;
  }

  public String getMemory() {
    return memory;
  }

  public void setMemory(String memory) {
    this.memory = memory;
  }

  public String getMemoryOverhead() {
    return memoryOverhead;
  }

  public void setMemoryOverhead(String memoryOverhead) {
    this.memoryOverhead = memoryOverhead;
  }

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public Map<String, String> getLabels() { return labels; }

  public void setLabels(Map<String, String> labels) { this.labels = labels; }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((coreLimit == null) ? 0 : coreLimit.hashCode());
    result = prime * result + ((cores == null) ? 0 : cores.hashCode());
    result = prime * result + ((image == null) ? 0 : image.hashCode());
    result = prime * result + ((instances == null) ? 0 : instances.hashCode());
    result = prime * result + ((labels == null) ? 0 : labels.hashCode());
    result = prime * result + ((memory == null) ? 0 : memory.hashCode());
    result = prime * result + ((memoryOverhead == null) ? 0 : memoryOverhead.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SparkApplicationExecutor other = (SparkApplicationExecutor) obj;
    if (coreLimit == null) {
      if (other.coreLimit != null) {
        return false;
      }
    } else if (!coreLimit.equals(other.coreLimit)) {
      return false;
    }
    if (cores == null) {
      if (other.cores != null) {
        return false;
      }
    } else if (!cores.equals(other.cores)) {
      return false;
    }
    if (image == null) {
      if (other.image != null) {
        return false;
      }
    } else if (!image.equals(other.image)) {
      return false;
    }
    if (instances == null) {
      if (other.instances != null) {
        return false;
      }
    } else if (!instances.equals(other.instances)) {
      return false;
    }
    if (labels == null) {
      if (other.labels != null) {
        return false;
      }
    } else if (!labels.equals(other.labels)) {
      return false;
    }
    if (memory == null) {
      if (other.memory != null) {
        return false;
      }
    } else if (!memory.equals(other.memory)) {
      return false;
    }
    if (memoryOverhead == null) {
      return other.memoryOverhead == null;
    } else {
      return memoryOverhead.equals(other.memoryOverhead);
    }
  }

}
