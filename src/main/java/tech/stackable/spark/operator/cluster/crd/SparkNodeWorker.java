package tech.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkNodeWorker extends SparkNode {

  private static final long serialVersionUID = -1742688274816192240L;

  public static final String POD_TYPE = "worker";

  public SparkNodeWorker() {
    setTypeName(POD_TYPE);
  }
}
