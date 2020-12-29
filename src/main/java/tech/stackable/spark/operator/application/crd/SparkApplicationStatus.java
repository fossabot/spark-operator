package tech.stackable.spark.operator.application.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize
@JsonInclude(Include.NON_NULL)
public class SparkApplicationStatus implements KubernetesResource {
  private static final long serialVersionUID = 7199753054202882887L;
}
