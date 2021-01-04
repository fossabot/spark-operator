package tech.stackable.spark.operator.cluster.manager.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize
@JsonInclude(Include.NON_NULL)
public class SparkManagerSpec implements KubernetesResource {

  private static final long serialVersionUID = -4637042703118011414L;

  private String sparkClusterReference;
  private String managerAction;

  public String getSparkClusterReference() {
    return sparkClusterReference;
  }

  public void setSparkClusterReference(String sparkClusterReference) {
    this.sparkClusterReference = sparkClusterReference;
  }

  public String getManagerAction() {
    return managerAction;
  }

  public void setManagerAction(String managerAction) {
    this.managerAction = managerAction;
  }

}
