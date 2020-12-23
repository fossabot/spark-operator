package tech.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
@JsonInclude(Include.NON_NULL)
public class SparkClusterStatusImage implements KubernetesResource {
	private static final long serialVersionUID = 6225543943942754250L;

	private String name;
	private String timestamp;
	
	public SparkClusterStatusImage() {}
	
	public SparkClusterStatusImage(String name, String timestamp) {
		super();
		this.name = name;
		this.timestamp = timestamp;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	public static class Builder {
		private String name;
		private String timestamp;
		
		public Builder withName(String name) {
			this.name = name;
			return this;
		}

		public Builder withTimeStamp(String timestamp) {
			this.timestamp = timestamp;
			return this;
		}
        
        public SparkClusterStatusImage build() {
        	SparkClusterStatusImage image =  new SparkClusterStatusImage(name, timestamp);
        	validateObject(image);
            return image;
        }
        
        private void validateObject(SparkClusterStatusImage image) {}
	}
}
