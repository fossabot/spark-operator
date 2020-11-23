package com.stackable.spark.operator.application;

import com.stackable.spark.operator.application.crd.SparkApplicationSpec;

import io.fabric8.kubernetes.client.CustomResource;

public class SparkApplication extends CustomResource {
	private static final long serialVersionUID = -4782526536451603317L;

	SparkApplicationSpec spec;

	public SparkApplicationSpec getSpec() {
		return spec;
	}

	public void setSpec(SparkApplicationSpec spec) {
		this.spec = spec;
	}
	
}
