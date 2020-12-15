package com.stackable.spark.operator.abstractcontroller.crd;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class CrdClassDoneable<T extends CustomResource> extends CustomResourceDoneable<T>{
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public CrdClassDoneable(T resource, Function function) {
        super(resource, function);
    }
}
