package com.stackable.spark.operator.application.crd;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public class SparkApplicationSpec implements KubernetesResource {
	private static final long serialVersionUID = 7517905868632922670L;

	private String image;
	private String mainApplicationFile;
	private String mainClass;
	private String sparkConfigMap;
	private String type;
	private String mode;
	private SparkApplicationDriver driver;
	private SparkApplicationExecutor executor;
	private List<String> dependencies = new ArrayList<String>();
	private List<String> args = new ArrayList<String>();
	
	public String getImage() {
		return image;
	}
	
	public void setImage(String image) {
		this.image = image;
	}
	
	public String getMainApplicationFile() {
		return mainApplicationFile;
	}
	
	public void setMainApplicationFile(String mainApplicationFile) {
		this.mainApplicationFile = mainApplicationFile;
	}
	
	public String getMainClass() {
		return mainClass;
	}
	
	public void setMainClass(String mainClass) {
		this.mainClass = mainClass;
	}
	
	public String getSparkConfigMap() {
		return sparkConfigMap;
	}
	
	public void setSparkConfigMap(String sparkConfigMap) {
		this.sparkConfigMap = sparkConfigMap;
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public String getMode() {
		return mode;
	}
	
	public void setMode(String mode) {
		this.mode = mode;
	}
	
	public SparkApplicationDriver getDriver() {
		return driver;
	}
	
	public void setDriver(SparkApplicationDriver driver) {
		this.driver = driver;
	}
	
	public SparkApplicationExecutor getExecutor() {
		return executor;
	}
	
	public void setExecutor(SparkApplicationExecutor executor) {
		this.executor = executor;
	}
	
	public List<String> getDependencies() {
		return dependencies;
	}
	
	public void setDependencies(List<String> dependencies) {
		this.dependencies = dependencies;
	}
	
	public List<String> getArgs() {
		return args;
	}
	
	public void setArgs(List<String> args) {
		this.args = args;
	}
	
}
