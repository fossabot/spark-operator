package com.stackable.spark.operator.cluster.crd.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionStatus;

@JsonDeserialize(using = JsonDeserializer.None.class)
@JsonInclude(Include.NON_NULL)
public class SparkClusterStatus extends CustomResourceDefinitionStatus {
	private static final long serialVersionUID = -948085681809118449L;
	
	private SparkClusterSystemdStatus systemd;
	private SparkClusterImageStatus image;
	
	public SparkClusterSystemdStatus getSystemd() {
		return systemd;
	}
	
	public void setSystemd(SparkClusterSystemdStatus systemd) {
		this.systemd = systemd;
	}

	public SparkClusterImageStatus getImage() {
		return image;
	}

	public void setImage(SparkClusterImageStatus image) {
		this.image = image;
	}
}
