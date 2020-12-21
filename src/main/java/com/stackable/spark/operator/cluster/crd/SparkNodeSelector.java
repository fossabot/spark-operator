package com.stackable.spark.operator.cluster.crd;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkNodeSelector implements KubernetesResource{
	private static final long serialVersionUID = 2535064095918732663L;

	private Integer instances = 1;
	private String memory;
	private String cores;
	private Map<String,String> matchLabels = new HashMap<String,String>();

	public Integer getInstances() {
		return instances;
	}

	public void setInstances(Integer instances) {
		this.instances = instances;
	}

	public String getMemory() {
		return memory;
	}

	public void setMemory(String memory) {
		this.memory = memory;
	}

	public String getCores() {
		return cores;
	}

	public void setCores(String cores) {
		this.cores = cores;
	}

	public Map<String,String> getMatchLabels() {
		return matchLabels;
	}

	public void setMatchLabels(Map<String,String> matchLabels) {
		this.matchLabels = matchLabels;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cores == null) ? 0 : cores.hashCode());
		result = prime * result + ((instances == null) ? 0 : instances.hashCode());
		result = prime * result + ((matchLabels == null) ? 0 : matchLabels.hashCode());
		result = prime * result + ((memory == null) ? 0 : memory.hashCode());
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
		SparkNodeSelector other = (SparkNodeSelector) obj;
		if (cores == null) {
			if (other.cores != null)
				return false;
		} else if (!cores.equals(other.cores))
			return false;
		if (instances == null) {
			if (other.instances != null)
				return false;
		} else if (!instances.equals(other.instances))
			return false;
		if (matchLabels == null) {
			if (other.matchLabels != null)
				return false;
		} else if (!matchLabels.equals(other.matchLabels))
			return false;
		if (memory == null) {
			if (other.memory != null)
				return false;
		} else if (!memory.equals(other.memory))
			return false;
		return true;
	}
	
}
