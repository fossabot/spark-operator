package com.stackable.spark.operator.application.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkApplicationDriver implements KubernetesResource {
	private static final long serialVersionUID = 1451406324355746300L;

	private String cores;
	private String coreLimit;
	private String memory;
	private String memoryOverhead;
	private String image;
	private String serviceAccount;
	
	public String getCores() {
		return cores;
	}
	
	public void setCores(String cores) {
		this.cores = cores;
	}
	
	public String getCoreLimit() {
		return coreLimit;
	}
	
	public void setCoreLimit(String coreLimit) {
		this.coreLimit = coreLimit;
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

}
