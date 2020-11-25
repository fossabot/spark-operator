package com.stackable.spark.operator.application.crd;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkApplicationDriver implements KubernetesResource {
	private static final long serialVersionUID = 1451406324355746300L;

	private String cores;
	private String coreLimits;
	private String memory;
	private String memoryOverhead;
	private String image;
	private String serviceAccount;
	private Map<String,String> labels = new HashMap<String,String>();
	
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

	public String getMemory() {
		return memory;
	}

	public void setMemory(String memory) {
		this.memory = memory;
	}

	public String getMemoryOverhead() {
		return memoryOverhead;
	}

	public void setMemoryOverhead(String memoryOverhead) {
		this.memoryOverhead = memoryOverhead;
	}
	
	public String getImage() {
		return image;
	}

	public void setImage(String image) {
		this.image = image;
	}

	public String getServiceAccount() {
		return serviceAccount;
	}

	public void setServiceAccount(String serviceAccount) {
		this.serviceAccount = serviceAccount;
	}

	public Map<String, String> getLabels() {
		return labels;
	}

	public void setLabels(Map<String, String> labels) {
		this.labels = labels;
	}
	
}
