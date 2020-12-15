package com.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.Toleration;

@JsonDeserialize(using = JsonDeserializer.None.class)
public abstract class SparkNode implements KubernetesResource {
	private static final long serialVersionUID = 5917995090358580518L;

	@JsonIgnore
	private String podTypeName;

	private Integer instances = 1;
	private String memory = "1000m";
	private String cores = "1";
	private List<SparkSelector> selectors = new ArrayList<SparkSelector>();
	private List<Toleration> tolerations = new ArrayList<Toleration>();
	private List<String> commands = new ArrayList<String>();
	private List<String> args = new ArrayList<String>();
	private List<EnvVar> sparkConfiguration = new ArrayList<EnvVar>();
	private List<EnvVar> env = new ArrayList<EnvVar>();
	
    public String getPodTypeName() {
		return podTypeName;
	}

	public void setTypeName(String typeName) {
		this.podTypeName = typeName;
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

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public java.lang.String getCores() {
        return cores;
    }

    public void setCores(String cores) {
        this.cores = cores;
    }
    
	public List<SparkSelector> getSelectors() {
		return selectors;
	}

	public void setSelectors(List<SparkSelector> selectors) {
		this.selectors = selectors;
	}
	
    public List<Toleration> getTolerations() {
		return tolerations;
	}

	public void setTolerations(List<Toleration> tolerations) {
		this.tolerations = tolerations;
	}

	public List<String> getCommands() {
        return commands;
    }

    public void setCommands(List<String> commands) {
        this.commands = commands;
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
	
	public List<EnvVar> getSparkConfiguration() {
		return sparkConfiguration;
	}

	public void setSparkConfiguration(List<EnvVar> sparkConfiguration) {
		this.sparkConfiguration = sparkConfiguration;
	}

	public List<EnvVar> getEnv() {
		return env;
	}

	public void setEnv(List<EnvVar> env) {
		this.env = env;
	}

}
