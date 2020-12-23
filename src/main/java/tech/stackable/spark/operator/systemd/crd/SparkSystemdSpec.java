package tech.stackable.spark.operator.systemd.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
@JsonInclude(Include.NON_NULL)
public class SparkSystemdSpec implements KubernetesResource {
	private static final long serialVersionUID = -4637042703118011414L;

	private String sparkClusterReference;
	private String systemdAction;
	
	public String getSparkClusterReference() {
		return sparkClusterReference;
	}
	
	public void setSparkClusterReference(String sparkClusterReference) {
		this.sparkClusterReference = sparkClusterReference;
	}
	
	public String getSystemdAction() {
		return systemdAction;
	}
	
	public void setSystemdAction(String systemdAction) {
		this.systemdAction = systemdAction;
	}
	
}
