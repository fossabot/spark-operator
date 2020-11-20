package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
public class Master extends SparkNode {
	private static final long serialVersionUID = 5917995090358580518L;
	
	public Master() {
		this.setPodName("master");
	}
}
