package com.stackable.spark.operator.cluster;

import com.stackable.spark.operator.abstractcontroller.crd.CrdClass;
import com.stackable.spark.operator.cluster.crd.SparkClusterSpec;
import com.stackable.spark.operator.cluster.crd.SparkClusterStatus;

public class SparkCluster extends CrdClass<SparkClusterSpec,SparkClusterStatus> {
	private static final long serialVersionUID = -6170886307484733035L;
}
