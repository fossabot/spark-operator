package com.stackable.spark.operator.application;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class SparkApplicationDoneable extends CustomResourceDoneable<SparkApplication> {
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public SparkApplicationDoneable(SparkApplication resource, Function function) { super(resource, function); }
}