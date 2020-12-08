package com.stackable.spark.operator.systemd;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class SparkSystemdDoneable extends CustomResourceDoneable<SparkSystemd> {
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public SparkSystemdDoneable(SparkSystemd resource, Function function) { super(resource, function); }
}
