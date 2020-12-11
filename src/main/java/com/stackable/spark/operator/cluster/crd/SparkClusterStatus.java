package com.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionStatus;

@JsonDeserialize(using = JsonDeserializer.None.class)
@JsonInclude(Include.NON_NULL)
public class SparkClusterStatus extends CustomResourceDefinitionStatus {
	private static final long serialVersionUID = -948085681809118449L;
	
	private SparkClusterCommand runningCommand;
	private List<String> stagedCommands;
	
	public SparkClusterStatus() {}
	
	public SparkClusterStatus(SparkClusterCommand runningCommand, List<String> stagedCommands) {
		super();
		this.runningCommand = runningCommand;
		this.stagedCommands = stagedCommands;
	}

	public SparkClusterCommand getRunningCommand() {
		return runningCommand;
	}
	
	public void setRunningCommands(SparkClusterCommand runningCommand) {
		this.runningCommand = runningCommand;
	}
	
	public List<String> getStagedCommands() {
		return stagedCommands;
	}
	
	public void setStagedCommands(List<String> stagedCommands) {
		this.stagedCommands = stagedCommands;
	}
	
	public static class Builder {
		private SparkClusterCommand runningCommand;
		private List<String> stagedCommands;
		
        public Builder withRunningCommands(SparkClusterCommand runningCommand) {
        	this.runningCommand = runningCommand;
        	return this;
        }
        
        public Builder withStagedCommands(List<String> stagedCommands) {
        	this.stagedCommands = stagedCommands;
        	return this;
        }
        
        public Builder withSingleRunningCommand(SparkClusterCommand runningCommand) {
        	this.runningCommand = runningCommand;
        	return this;
        }
        
        public Builder withSingleStagedCommand(String stagedCommand) {
        	if(this.stagedCommands == null) {
        		this.stagedCommands = new ArrayList<String>();
        	}
        	this.stagedCommands.add(stagedCommand);
        	return this;
        }
        
        public SparkClusterStatus build() {
        	SparkClusterStatus status =  new SparkClusterStatus(runningCommand, stagedCommands);
        	validateObject(status);
            return status;
        }
        
        private void validateObject(SparkClusterStatus status) {}
	}
	
}
