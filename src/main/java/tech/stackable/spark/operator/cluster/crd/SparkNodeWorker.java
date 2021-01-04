package tech.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize
public class SparkNodeWorker extends SparkNode {

  private static final long serialVersionUID = -1742688274816192240L;

  public SparkNodeWorker() {
    setTypeName(SparkNodeType.WORKER);
  }
}
