package com.stackable.spark.operator.application.crd;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkApplicationExecutor implements KubernetesResource {
	private static final long serialVersionUID = 5905650793866264723L;

	private Integer instances;
	private String cores;
	private String coreLimit;
	private String memory;
	private String memoryOverhead;
	private String image;
	private Map<String,String> labels = new HashMap<String,String>();

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

	public Map<String, String> getLabels() {
		return labels;
	}

	public void setLabels(Map<String, String> labels) {
		this.labels = labels;
	}
	
}
