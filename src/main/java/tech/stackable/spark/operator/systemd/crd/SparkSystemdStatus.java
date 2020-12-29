package tech.stackable.spark.operator.systemd.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionStatus;

@JsonDeserialize
@JsonInclude(Include.NON_NULL)
public class SparkSystemdStatus extends CustomResourceDefinitionStatus {

  private static final long serialVersionUID = -3689504450554921281L;
}
