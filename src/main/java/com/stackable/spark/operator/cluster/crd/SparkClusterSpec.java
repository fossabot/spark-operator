package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkClusterSpec implements KubernetesResource {
	private static final long serialVersionUID = -4949229889562573739L;
	
	private Master master;
	private Worker worker;
    private Boolean metrics;
	
	public Master getMaster() {
		return master;
	}

	public void setMaster(Master master) {
		this.master = master;
	}

	public Worker getWorker() {
		return worker;
	}

	public void setWorker(Worker worker) {
		this.worker = worker;
	}

	public Boolean getMetrics() {
		return metrics;
	}

	public void setMetrics(Boolean metrics) {
		this.metrics = metrics;
	}

}
