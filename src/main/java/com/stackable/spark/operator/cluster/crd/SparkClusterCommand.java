package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
@JsonInclude(Include.NON_NULL)
public class SparkClusterCommand implements KubernetesResource {
	private static final long serialVersionUID = -8101216638321166890L;

	// preset for status update operation
	private String command;
	private String timeStamp;
	private String startedAt;
	private String finishedAt;
	private String status;
	
	public SparkClusterCommand() {}
	
	public SparkClusterCommand(String command, String timeStamp, String startedAt, String finishedAt, String status) {
		super();
		this.command = command;
		this.timeStamp = timeStamp;
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
	
	public String getTimeStamp() {
		return timeStamp;
	}
	
	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
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
		private String timeStamp;
		private String startedAt;
		private String finishedAt;
		private String status;
		
		public Builder withCommand(String command) {
			this.command = command;
			return this;
		}

		public Builder withTimeStamp(String timeStamp) {
			this.timeStamp = timeStamp;
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
        
        public SparkClusterCommand build() {
        	SparkClusterCommand clusterCommand =  new SparkClusterCommand(command, timeStamp, startedAt, finishedAt, status);
        	validateObject(clusterCommand);
            return clusterCommand;
        }
        
        private void validateObject(SparkClusterCommand command) {}
	}
	
}