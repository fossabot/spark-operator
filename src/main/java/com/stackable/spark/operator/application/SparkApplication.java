package com.stackable.spark.operator.application;

import com.stackable.spark.operator.application.crd.SparkApplicationSpec;

import io.fabric8.kubernetes.client.CustomResource;

public class SparkApplication extends CustomResource {
	private static final long serialVersionUID = -4782526536451603317L;

	SparkApplicationSpec spec;

	public SparkApplicationSpec getSpec() {
		return spec;
	}

	public void setSpec(SparkApplicationSpec spec) {
		this.spec = spec;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((spec == null) ? 0 : spec.hashCode());
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
		SparkApplication other = (SparkApplication) obj;
		if (spec == null) {
			if (other.spec != null)
				return false;
		} else if (!spec.equals(other.spec))
			return false;
		return true;
	}
	
}
