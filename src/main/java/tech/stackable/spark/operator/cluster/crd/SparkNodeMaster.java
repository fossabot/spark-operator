package tech.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize
public class SparkNodeMaster extends SparkNode {

  private static final long serialVersionUID = 5917995090358580518L;

  public SparkNodeMaster() {
    setTypeName(SparkNodeType.MASTER);
  }
}
