package tech.stackable.spark.operator.systemd;

import tech.stackable.spark.operator.abstractcontroller.crd.CrdClass;
import tech.stackable.spark.operator.systemd.crd.SparkSystemdSpec;
import tech.stackable.spark.operator.systemd.crd.SparkSystemdStatus;

public class SparkSystemd extends CrdClass<SparkSystemdSpec, SparkSystemdStatus> {

  private static final long serialVersionUID = 6483276578253436209L;
}
