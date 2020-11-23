package com.stackable.spark.operator.application.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkApplicationExecutor extends SparkApplicationDriver {
	private static final long serialVersionUID = 5905650793866264723L;

	private Integer instances;

	public Integer getInstances() {
		return instances;
	}

	public void setInstances(Integer instances) {
		this.instances = instances;
	}
	
}
