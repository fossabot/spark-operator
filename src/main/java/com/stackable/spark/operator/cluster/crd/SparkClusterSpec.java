package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkClusterSpec implements KubernetesResource {
	private static final long serialVersionUID = -4949229889562573739L;
	
	private SparkNodeMaster master;
	private SparkNodeWorker worker;
    private Boolean metrics;
	
	public SparkNodeMaster getMaster() {
		return master;
	}

	public void setMaster(SparkNodeMaster master) {
		this.master = master;
	}

	public SparkNodeWorker getWorker() {
		return worker;
	}

	public void setWorker(SparkNodeWorker worker) {
		this.worker = worker;
	}

	public Boolean getMetrics() {
		return metrics;
	}

	public void setMetrics(Boolean metrics) {
		this.metrics = metrics;
	}

}
