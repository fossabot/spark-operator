package com.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
public class SparkClusterSpec implements KubernetesResource {
	private static final long serialVersionUID = -4949229889562573739L;
	
	private Master master;
	private Worker worker;
    private Boolean metrics;
    private Boolean sparkWebUI;
    private String sparkConfigurationMap;
    private List<String> sparkConfiguration = new ArrayList<String>();
    private List<String> env = new ArrayList<String>();
    // TODO: add
//    private HistoryServer historyServer;
	
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

	public Boolean getSparkWebUI() {
		return sparkWebUI;
	}

	public void setSparkWebUI(Boolean sparkWebUI) {
		this.sparkWebUI = sparkWebUI;
	}

	public String getSparkConfigurationMap() {
		return sparkConfigurationMap;
	}

	public void setSparkConfigurationMap(String sparkConfigurationMap) {
		this.sparkConfigurationMap = sparkConfigurationMap;
	}

	public List<String> getSparkConfiguration() {
		return sparkConfiguration;
	}

	public void setSparkConfiguration(List<String> sparkConfiguration) {
		this.sparkConfiguration = sparkConfiguration;
	}

	public List<String> getEnv() {
		return env;
	}

	public void setEnv(List<String> env) {
		this.env = env;
	}
	
}
