package tech.stackable.spark.operator.cluster;

import tech.stackable.spark.operator.abstractcontroller.crd.CrdClass;
import tech.stackable.spark.operator.cluster.crd.SparkClusterSpec;
import tech.stackable.spark.operator.cluster.crd.SparkClusterStatus;

public class SparkCluster extends CrdClass<SparkClusterSpec,SparkClusterStatus> {
	private static final long serialVersionUID = -6170886307484733035L;
}
