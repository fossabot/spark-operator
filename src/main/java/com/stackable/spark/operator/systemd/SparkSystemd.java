package com.stackable.spark.operator.systemd;

import com.stackable.spark.operator.systemd.crd.SparkSystemdSpec;

import io.fabric8.kubernetes.client.CustomResource;

public class SparkSystemd extends CustomResource {
	private static final long serialVersionUID = 6483276578253436209L;
	
	private SparkSystemdSpec spec;

	public SparkSystemdSpec getSpec() {
		return spec;
	}

	public void setSpec(SparkSystemdSpec spec) {
		this.spec = spec;
	}
	
}
