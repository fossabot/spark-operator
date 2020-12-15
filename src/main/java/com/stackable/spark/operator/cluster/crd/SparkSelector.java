package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkSelector implements KubernetesResource{
	private static final long serialVersionUID = 2535064095918732663L;

	private SparkSelectorMatch selector;

	public SparkSelectorMatch getSelector() {
		return selector;
	}

	public void setSelector(SparkSelectorMatch selector) {
		this.selector = selector;
	}

}
