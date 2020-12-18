package com.stackable.spark.operator.common.fabric8;

import com.stackable.spark.operator.application.SparkApplication;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class SparkApplicationDoneable extends CustomResourceDoneable<SparkApplication> {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public SparkApplicationDoneable(SparkApplication application, Function function) {
        super(application, function);
	}
}

