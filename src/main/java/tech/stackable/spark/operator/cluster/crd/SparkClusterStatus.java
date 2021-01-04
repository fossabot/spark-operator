package tech.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionStatus;

@JsonDeserialize
@JsonInclude(Include.NON_NULL)
public class SparkClusterStatus extends CustomResourceDefinitionStatus {

  private static final long serialVersionUID = -948085681809118449L;

  private SparkClusterStatusManager manager;
  private SparkClusterStatusImage image;

  public SparkClusterStatus() {
  }

  private SparkClusterStatus(SparkClusterStatusManager manager, SparkClusterStatusImage image) {
    this.manager = manager;
    this.image = image;
  }

  public SparkClusterStatusManager getManager() {
    return manager;
  }

  public void setManager(SparkClusterStatusManager manager) {
    this.manager = manager;
  }

  public SparkClusterStatusImage getImage() {
    return image;
  }

  public void setImage(SparkClusterStatusImage image) {
    this.image = image;
  }

  public static class Builder {

    private SparkClusterStatusManager manager;
    private SparkClusterStatusImage image;

    public Builder withSystemdStatus(SparkClusterStatusManager manager) {
      this.manager = manager;
      return this;
    }

    public Builder withClusterImageStatus(SparkClusterStatusImage image) {
      this.image = image;
      return this;
    }

    public SparkClusterStatus build() {
      return new SparkClusterStatus(manager, image);
    }

  }
}
