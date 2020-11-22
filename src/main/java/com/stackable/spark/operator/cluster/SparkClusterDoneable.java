package com.stackable.spark.operator.cluster;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class SparkClusterDoneable extends CustomResourceDoneable<SparkCluster> {
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public SparkClusterDoneable(SparkCluster resource, Function function) { super(resource, function); }
}
