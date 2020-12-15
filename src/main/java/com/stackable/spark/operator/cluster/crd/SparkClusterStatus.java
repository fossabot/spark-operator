package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionStatus;

@JsonDeserialize(using = JsonDeserializer.None.class)
@JsonInclude(Include.NON_NULL)
public class SparkClusterStatus extends CustomResourceDefinitionStatus {
	private static final long serialVersionUID = -948085681809118449L;
	
	private SparkClusterStatusSystemd systemd;
	private SparkClusterStatusImage image;
	
	public SparkClusterStatusSystemd getSystemd() {
		return systemd;
	}
	
	public void setSystemd(SparkClusterStatusSystemd systemd) {
		this.systemd = systemd;
	}

	public SparkClusterStatusImage getImage() {
		return image;
	}

	public void setImage(SparkClusterStatusImage image) {
		this.image = image;
	}
}
