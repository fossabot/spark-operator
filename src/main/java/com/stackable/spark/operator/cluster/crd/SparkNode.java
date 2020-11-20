package com.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
public abstract class SparkNode implements KubernetesResource {
	private static final long serialVersionUID = 5917995090358580518L;

	@JsonIgnore
	private String podName;

    private Integer instances = 1;
    private String memory;
    private String cpu;
    private List<String> command = new ArrayList<String>();
    private List<String> args = new ArrayList<String>();
    private String image;

    public String getPodName() {
		return podName;
	}

	public void setPodName(String podName) {
		this.podName = podName;
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

	public String getImage() {
		return image;
	}

	public void setImage(String image) {
		this.image = image;
	}

	@Override
    public java.lang.String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(SparkNode.class.getName());
        sb.append('@');
        sb.append(Integer.toHexString(System.identityHashCode(this)));
        sb.append('[');
        sb.append("instances");
        sb.append('=');
        sb.append(((this.instances == null) ? "" : this.instances));
        sb.append(',');
        sb.append("memory");
        sb.append('=');
        sb.append(((this.memory == null) ? "" : this.memory));
        sb.append(',');
        sb.append("cpu");
        sb.append('=');
        sb.append(((this.cpu == null) ? "" : this.cpu));
        sb.append(',');
        sb.append("command");
        sb.append('=');
        sb.append(((this.command == null) ? "" : this.command));
        sb.append(',');
        sb.append("args");
        sb.append('=');
        sb.append(((this.args == null) ? "" : this.args));
        sb.append(',');
        sb.append("image");
        sb.append('=');
        sb.append(((this.image == null) ? "" : this.image));
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((args == null) ? 0 : args.hashCode());
		result = prime * result + ((command == null) ? 0 : command.hashCode());
		result = prime * result + ((cpu == null) ? 0 : cpu.hashCode());
		result = prime * result + ((image == null) ? 0 : image.hashCode());
		result = prime * result + ((instances == null) ? 0 : instances.hashCode());
		result = prime * result + ((memory == null) ? 0 : memory.hashCode());
		result = prime * result + ((podName == null) ? 0 : podName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SparkNode other = (SparkNode) obj;
		if (args == null) {
			if (other.args != null)
				return false;
		} else if (!args.equals(other.args))
			return false;
		if (command == null) {
			if (other.command != null)
				return false;
		} else if (!command.equals(other.command))
			return false;
		if (cpu == null) {
			if (other.cpu != null)
				return false;
		} else if (!cpu.equals(other.cpu))
			return false;
		if (image == null) {
			if (other.image != null)
				return false;
		} else if (!image.equals(other.image))
			return false;
		if (instances == null) {
			if (other.instances != null)
				return false;
		} else if (!instances.equals(other.instances))
			return false;
		if (memory == null) {
			if (other.memory != null)
				return false;
		} else if (!memory.equals(other.memory))
			return false;
		if (podName == null) {
			if (other.podName != null)
				return false;
		} else if (!podName.equals(other.podName))
			return false;
		return true;
	}
}
