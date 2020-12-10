package com.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionStatus;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkClusterStatus extends CustomResourceDefinitionStatus {
	private static final long serialVersionUID = -948085681809118449L;
	
	private List<SparkClusterCommand> runningCommands;
	private List<SparkClusterCommand> stagedCommands;
	
	public SparkClusterStatus(List<SparkClusterCommand> runningCommands, List<SparkClusterCommand> stagedCommands) {
		super();
		this.runningCommands = runningCommands;
		this.stagedCommands = stagedCommands;
	}

	public List<SparkClusterCommand> getRunningCommands() {
		return runningCommands;
	}
	
	public void setRunningCommands(List<SparkClusterCommand> runningCommands) {
		this.runningCommands = runningCommands;
	}
	
	public List<SparkClusterCommand> getStagedCommands() {
		return stagedCommands;
	}
	
	public void setStagedCommands(List<SparkClusterCommand> stagedCommands) {
		this.stagedCommands = stagedCommands;
	}
	
	public static class Builder {
		private List<SparkClusterCommand> runningCommands;
		private List<SparkClusterCommand> stagedCommands;
		
        public Builder withRunningCommands(List<SparkClusterCommand> runningCommands) {
        	this.runningCommands = runningCommands;
        	return this;
        }
        
        public Builder withStagedCommands(List<SparkClusterCommand> stagedCommands) {
        	this.stagedCommands = stagedCommands;
        	return this;
        }
        
        public Builder withSingleRunningCommand(SparkClusterCommand runningCommand) {
        	if(this.runningCommands == null) {
        		this.runningCommands = new ArrayList<SparkClusterCommand>();
        	}
        	this.runningCommands.add(runningCommand);
        	return this;
        }
        
        public Builder withSingleStagedCommand(SparkClusterCommand stagedCommand) {
        	if(this.stagedCommands == null) {
        		this.stagedCommands = new ArrayList<SparkClusterCommand>();
        	}
        	this.stagedCommands.add(stagedCommand);
        	return this;
        }
        
        public SparkClusterStatus build() {
        	SparkClusterStatus status =  new SparkClusterStatus(runningCommands, stagedCommands);
        	validateObject(status);
            return status;
        }
        
        private void validateObject(SparkClusterStatus status) {}
	}
	
}
