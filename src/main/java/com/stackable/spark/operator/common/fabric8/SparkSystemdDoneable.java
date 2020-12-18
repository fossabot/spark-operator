package com.stackable.spark.operator.common.fabric8;

import com.stackable.spark.operator.systemd.SparkSystemd;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class SparkSystemdDoneable extends CustomResourceDoneable<SparkSystemd> {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public SparkSystemdDoneable(SparkSystemd systemd, Function function) {
        super(systemd, function);
	}
}

