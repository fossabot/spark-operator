package com.stackable.spark.operator.application.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkApplicationExecutor implements KubernetesResource {
	private static final long serialVersionUID = 5905650793866264723L;

	private Integer instances;
	private String cores;
	private String coreLimits;

	public Integer getInstances() {
		return instances;
	}

	public void setInstances(Integer instances) {
		this.instances = instances;
	}

	public String getCores() {
		return cores;
	}

	public void setCores(String cores) {
		this.cores = cores;
	}

	public String getCoreLimits() {
		return coreLimits;
	}

	public void setCoreLimits(String coreLimits) {
		this.coreLimits = coreLimits;
	}
	
}
