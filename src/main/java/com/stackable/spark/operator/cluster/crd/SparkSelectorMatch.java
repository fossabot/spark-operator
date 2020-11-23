package com.stackable.spark.operator.cluster.crd;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkSelectorMatch implements KubernetesResource {
	private static final long serialVersionUID = 7831325703615732467L;
	
	private Map<String,String> matchLabels = new HashMap<String, String>();
	private Integer instances = 1;
	
	public Map<String, String> getMatchLabels() {
		return matchLabels;
	}
	
	public void setMatchLabels(Map<String, String> matchLabels) {
		this.matchLabels = matchLabels;
	}
	
	public Integer getInstances() {
		return instances;
	}
	
	public void setInstances(Integer instances) {
		this.instances = instances;
	}

}
