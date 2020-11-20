package com.stackable.spark.operator.cluster;

import com.stackable.spark.operator.cluster.crd.SparkClusterSpec;

import io.fabric8.kubernetes.client.CustomResource;

public class SparkCluster extends CustomResource {
	private static final long serialVersionUID = -6170886307484733035L;

	private SparkClusterSpec spec;
    
    public SparkClusterSpec getSpec() {
        return spec;
    }

    public void setSpec(SparkClusterSpec spec) {
        this.spec = spec;
    }

}
