package com.stackable.spark.operator.cluster.crd.status;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
@JsonInclude(Include.NON_NULL)
public class SparkClusterSystemdStatus implements KubernetesResource {
	private static final long serialVersionUID = 7259902757939136149L;
	
	private SparkClusterCommand runningCommand;
	private List<String> stagedCommands;
	
	public SparkClusterSystemdStatus() {}
	
	public SparkClusterSystemdStatus(SparkClusterCommand runningCommand, List<String> stagedCommands) {
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
	
	public void setRunningCommand(SparkClusterCommand runningCommand) {
		this.runningCommand = runningCommand;
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
        
        public SparkClusterSystemdStatus build() {
        	SparkClusterSystemdStatus status = new SparkClusterSystemdStatus(runningCommand, stagedCommands);
        	validateObject(status);
            return status;
        }
        
        private void validateObject(SparkClusterSystemdStatus status) {}
	}
}
