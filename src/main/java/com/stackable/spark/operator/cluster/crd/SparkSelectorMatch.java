package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkSelectorMatch implements KubernetesResource{
	private static final long serialVersionUID = 7831325703615732467L;
	
	private SparkSelectorMatchLabel matchLabels;
	private Integer instances = 1;

	public SparkSelectorMatchLabel getMatchLabels() {
		return matchLabels;
	}

	public void setMatchLabels(SparkSelectorMatchLabel matchLabels) {
		this.matchLabels = matchLabels;
	}

	public Integer getInstances() {
		return instances;
	}

	public void setInstances(Integer instances) {
		this.instances = instances;
	}

}
