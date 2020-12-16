package com.stackable.spark.operator.systemd;

import com.stackable.spark.operator.abstractcontroller.crd.CrdClass;
import com.stackable.spark.operator.systemd.crd.SparkSystemdSpec;
import com.stackable.spark.operator.systemd.crd.SparkSystemdStatus;

public class SparkSystemd extends CrdClass<SparkSystemdSpec,SparkSystemdStatus> {
	private static final long serialVersionUID = 6483276578253436209L;
}
