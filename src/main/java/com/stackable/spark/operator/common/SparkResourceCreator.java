package com.stackable.spark.operator.common;

import java.io.InputStream;
import java.util.List;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;

public class SparkResourceCreator {
	
    public static List<HasMetadata> createOrReplaceCRD(KubernetesClient client, String namespace, String path) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
    	List<HasMetadata> result = client.load(is).get();
    	client.resourceList(result).inNamespace(namespace).createOrReplace(); 
    	return result;
    }
}
