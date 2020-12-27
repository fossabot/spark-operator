package tech.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkNodeMaster extends SparkNode {

  private static final long serialVersionUID = 5917995090358580518L;

  public static final String POD_TYPE = "master";

  public SparkNodeMaster() {
    setTypeName(POD_TYPE);
  }
}
