package com.stackable.spark.operator.controller;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.systemd.SparkSystemd;
import com.stackable.spark.operator.systemd.SparkSystemdDoneable;
import com.stackable.spark.operator.systemd.SparkSystemdList;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;

public class SparkSystemdController extends AbstractCrdController<SparkSystemd, SparkSystemdList, SparkSystemdDoneable> {
	public static final Logger logger = Logger.getLogger(SparkSystemdController.class.getName());
	
    public SparkSystemdController(
    		KubernetesClient client,
    		SharedInformerFactory informerFactory,
    		String namespace,
			String crdPath, 
			Long resyncCycle) {
		super(client, informerFactory, namespace, crdPath, resyncCycle);
	}

	@Override
	protected CustomResourceDefinitionContext getCrdContext() {
        return new CustomResourceDefinitionContext.Builder()
                .withVersion("v1")
                .withScope("Namespaced")
                .withGroup("spark.stackable.de")
                .withPlural("sparksystemds")
                .build();
	}

	@Override
	protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced());
		logger.info("SparkSystemd informer initialized ... waiting for changes");
	}
	
	@Override
	protected void process(SparkSystemd systemd) {
		logger.trace("Got CRD: " + systemd.getMetadata().getName());
		// signal systemd command to SparkClusterController
		
		
		// remove crd?
		//crdClient.inNamespace(namespace).withName(systemd.getMetadata().getName()).delete();
	}

}
