package com.stackable.spark.operator.cluster.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class Worker extends SparkNode {
	private static final long serialVersionUID = -1742688274816192240L;
	
	public Worker() {
		this.setPodName("worker");
	}
}