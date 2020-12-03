package com.stackable.spark.operator.controller;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.application.SparkApplication;
import com.stackable.spark.operator.application.SparkApplicationDoneable;
import com.stackable.spark.operator.application.SparkApplicationList;
import com.stackable.spark.operator.application.launcher.SparkApplicationLauncher;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;

public class SparkApplicationController extends 
	AbstractCrdController<SparkApplication, SparkApplicationList, SparkApplicationDoneable> {

	public static final Logger logger = Logger.getLogger(SparkApplicationController.class.getName());
	
	private SparkApplicationLauncher sparkApplicationLauncher;
	
	public SparkApplicationController(
		KubernetesClient client,
		SharedInformerFactory informerFactory,
		String namespace,
		String crdPath,
		Long resyncCycle) {
		
		super(client, informerFactory, namespace, crdPath, resyncCycle);
		
		sparkApplicationLauncher = new SparkApplicationLauncher();
	}

    protected CustomResourceDefinitionContext getCrdContext() {
        return new CustomResourceDefinitionContext.Builder()
            .withVersion("v1")
            .withScope("Namespaced")
            .withGroup("spark.stackable.de")
            .withPlural("sparkapplications")
            .build();
    }
    
	@Override
	protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced());
		logger.info("SparkApplication informer initialized ... waiting for changes");
	}
	
	@Override
	protected void process(SparkApplication app) {
		logger.info("Got CRD: " + app.getMetadata().getName());
		sparkApplicationLauncher.launch(app);
	}

}
