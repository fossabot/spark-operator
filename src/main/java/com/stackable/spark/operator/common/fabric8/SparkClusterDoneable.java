package com.stackable.spark.operator.common.fabric8;

import com.stackable.spark.operator.cluster.SparkCluster;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class SparkClusterDoneable extends CustomResourceDoneable<SparkCluster> {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public SparkClusterDoneable(SparkCluster cluster, Function function) {
        super(cluster, function);
	}
}
