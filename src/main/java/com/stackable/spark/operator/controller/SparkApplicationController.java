package com.stackable.spark.operator.controller;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.application.SparkApplication;
import com.stackable.spark.operator.application.SparkApplicationList;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;

public class SparkApplicationController extends AbstractCrdController<SparkApplication, SparkApplicationList>{
	public SparkApplicationController(
		KubernetesClient client,
		SharedInformerFactory informerFactory,
		CustomResourceDefinitionContext crdContext,
		String namespace,
		String crdPath,
		Long resyncCycle) {
		
		super(client, informerFactory, crdContext, namespace, crdPath, resyncCycle);
	}

	public static final Logger logger = Logger.getLogger(SparkApplicationController.class.getName());

	@Override
	protected void process(AbstractCrdController<SparkApplication, SparkApplicationList> controller, SparkApplication crd) {
		
	}

}
