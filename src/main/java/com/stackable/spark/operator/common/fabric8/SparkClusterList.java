package com.stackable.spark.operator.common.fabric8;

import com.stackable.spark.operator.cluster.SparkCluster;

import io.fabric8.kubernetes.client.CustomResourceList;

public class SparkClusterList extends CustomResourceList<SparkCluster> {
	private static final long serialVersionUID = -8453549963001365047L;
}
