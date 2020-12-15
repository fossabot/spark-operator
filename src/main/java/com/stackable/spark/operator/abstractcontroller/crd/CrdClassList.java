package com.stackable.spark.operator.abstractcontroller.crd;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResourceList;

@SuppressWarnings("serial")
public class CrdClassList<T extends HasMetadata> extends CustomResourceList<T> {
}
