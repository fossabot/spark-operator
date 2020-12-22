package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
@JsonInclude(Include.NON_NULL)
public class SparkClusterStatusCommand implements KubernetesResource {
	private static final long serialVersionUID = -8101216638321166890L;

	// preset for status update operation
	private String command;
	private String reason;
	private String startedAt;
	private String finishedAt;
	private String status;
	
	public SparkClusterStatusCommand() {}
	
	public SparkClusterStatusCommand(String command, String reason, String startedAt, String finishedAt, String status) {
		super();
		this.command = command;
		this.reason = reason;
		this.startedAt = startedAt;
		this.finishedAt = finishedAt;
		this.status = status;
	}

	public String getCommand() {
		return command;
	}
	
	public void setCommand(String command) {
		this.command = command;
	}
	
	public String getReason() {
		return reason;
	}
	
	public void setReason(String reason) {
		this.reason = reason;
	}
	
	public String getStartedAt() {
		return startedAt;
	}
	
	public void setStartedAt(String startedAt) {
		this.startedAt = startedAt;
	}
	
	public String getFinishedAt() {
		return finishedAt;
	}
	
	public void setFinishedAt(String finishedAt) {
		this.finishedAt = finishedAt;
	}
	
	public String getStatus() {
		return status;
	}
	
	public void setStatus(String status) {
		this.status = status;
	}
	
	public static class Builder {
		private String command;
		private String reason;
		private String startedAt;
		private String finishedAt;
		private String status;
		
		public Builder withCommand(String command) {
			this.command = command;
			return this;
		}

		public Builder withReason(String reason) {
			this.reason = reason;
			return this;
		}
        
		public Builder withStartedAt(String startedAt) {
			this.startedAt = startedAt;
			return this;
		}
        
		public Builder withFinishedAt(String finishedAt) {
			this.finishedAt = finishedAt;
			return this;
		}
		
		public Builder withStatus(String status) {
			this.status = status;
			return this;
		}
        
        public SparkClusterStatusCommand build() {
        	SparkClusterStatusCommand clusterCommand =  new SparkClusterStatusCommand(command, reason, startedAt, finishedAt, status);
        	validateObject(clusterCommand);
            return clusterCommand;
        }
        
        private void validateObject(SparkClusterStatusCommand command) {}
	}
	
}