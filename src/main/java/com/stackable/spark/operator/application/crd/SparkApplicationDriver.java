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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((coreLimit == null) ? 0 : coreLimit.hashCode());
		result = prime * result + ((cores == null) ? 0 : cores.hashCode());
		result = prime * result + ((image == null) ? 0 : image.hashCode());
		result = prime * result + ((memory == null) ? 0 : memory.hashCode());
		result = prime * result + ((memoryOverhead == null) ? 0 : memoryOverhead.hashCode());
		result = prime * result + ((serviceAccount == null) ? 0 : serviceAccount.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SparkApplicationDriver other = (SparkApplicationDriver) obj;
		if (coreLimit == null) {
			if (other.coreLimit != null)
				return false;
		} else if (!coreLimit.equals(other.coreLimit))
			return false;
		if (cores == null) {
			if (other.cores != null)
				return false;
		} else if (!cores.equals(other.cores))
			return false;
		if (image == null) {
			if (other.image != null)
				return false;
		} else if (!image.equals(other.image))
			return false;
		if (memory == null) {
			if (other.memory != null)
				return false;
		} else if (!memory.equals(other.memory))
			return false;
		if (memoryOverhead == null) {
			if (other.memoryOverhead != null)
				return false;
		} else if (!memoryOverhead.equals(other.memoryOverhead))
			return false;
		if (serviceAccount == null) {
			if (other.serviceAccount != null)
				return false;
		} else if (!serviceAccount.equals(other.serviceAccount))
			return false;
		return true;
	}
	
}
