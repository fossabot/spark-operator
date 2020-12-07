package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionStatus;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkClusterStatus extends CustomResourceDefinitionStatus {
	private static final long serialVersionUID = -948085681809118449L;
	
	private SparkClusterCommand runningCommand;
	private SparkClusterCommand stagedCommand;
	
	public SparkClusterCommand getRunningCommand() {
		return runningCommand;
	}
	
	public void setRunningCommand(SparkClusterCommand runningCommand) {
		this.runningCommand = runningCommand;
	}
	
	public SparkClusterCommand getStagedCommand() {
		return stagedCommand;
	}
	
	public void setStagedCommand(SparkClusterCommand stagedCommand) {
		this.stagedCommand = stagedCommand;
	}
	
}
