package com.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(using = JsonDeserializer.None.class)
public abstract class SparkNode implements KubernetesResource {
	private static final long serialVersionUID = 5917995090358580518L;

	@JsonIgnore
	private String typeName;

	private Integer instances = 1;
	private String memory;
	private String cpu;
	private List<SparkSelector> selectors = new ArrayList<SparkSelector>();
	private List<String> command = new ArrayList<String>();
	private List<String> args = new ArrayList<String>();
	private List<Map<String,String>> env = new ArrayList<Map<String,String>>();
	
    public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	
	public Integer getInstances() {
        return instances;
    }

	public void setInstances(Integer instances) {
        this.instances = instances;
    }

    public java.lang.String getMemory() {
        return memory;
    }

    public void setMemory(java.lang.String memory) {
        this.memory = memory;
    }

    public java.lang.String getCpu() {
        return cpu;
    }

    public void setCpu(java.lang.String cpu) {
        this.cpu = cpu;
    }
    
	public List<SparkSelector> getSelectors() {
		return selectors;
	}

	public void setSelectors(List<SparkSelector> selectors) {
		this.selectors = selectors;
	}
    
    public List<String> getCommand() {
        return command;
    }

    public void setCommand(List<String> command) {
        this.command = command;
    }

    public List<String> getCommandArgs() {
        return args;
    }

    public void setCommandArgs(List<String> commandArgs) {
        this.args = commandArgs;
    }

    public List<String> getArgs() {
		return args;
	}

	public void setArgs(List<String> args) {
		this.args = args;
	}

	public List<Map<String, String>> getEnv() {
		return env;
	}

	public void setEnv(List<Map<String, String>> env) {
		this.env = env;
	}

}
