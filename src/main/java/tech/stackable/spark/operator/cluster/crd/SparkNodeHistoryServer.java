package tech.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize
public class SparkNodeHistoryServer extends SparkNode {

  private static final long serialVersionUID = -8626713213445786145L;

  public SparkNodeHistoryServer() {
    setTypeName(SparkNodeType.HISTORY_SERVER);
  }
}
