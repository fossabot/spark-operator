package com.stackable.spark.operator.application.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkApplicationDriver implements KubernetesResource {
	private static final long serialVersionUID = 1451406324355746300L;

	private String cores;
	private String coreLimits;
	
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
