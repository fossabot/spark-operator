package com.stackable.spark.operator.controller;

import java.lang.reflect.ParameterizedType;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Lister;

public abstract class AbstractCrdController<CRDClass extends CustomResource, 
											CRDClassList extends CustomResourceList<CRDClass>> {
	protected KubernetesClient client;
	protected String namespace;
	
	protected SharedIndexInformer<CRDClass> crdSharedIndexInformer;
	protected Lister<CRDClass> crdLister;
	
	@SuppressWarnings("unchecked")
	public AbstractCrdController(
			KubernetesClient client,
			SharedInformerFactory informerFactory,
			CustomResourceDefinitionContext crdContext,
			String namespace,
			Long resyncCycle) {
		
		this.client = client;
		this.namespace = namespace;

		this.crdSharedIndexInformer = informerFactory.sharedIndexInformerForCustomResource(
			crdContext, 
        	(Class<CRDClass>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], 
        	(Class<CRDClassList>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1], 
        	resyncCycle
        ); 
		
		this.crdLister = new Lister<>(crdSharedIndexInformer.getIndexer(), namespace);
		
		// register events handlers
		registerCrdEventHandler();
	}
	
    protected abstract void onCrdAdd(CRDClass crd);
    protected abstract void onCrdUpdate(CRDClass crdOld, CRDClass crdNew);
    protected abstract void onCrdDelete(CRDClass crd, boolean deletedFinalStateUnknown);
    
    protected void registerCrdEventHandler() {
        this.crdSharedIndexInformer.addEventHandler(new ResourceEventHandler<CRDClass>() {
            @Override
            public void onAdd(CRDClass crd) {
            	onCrdAdd(crd);
            }

            @Override
            public void onUpdate(CRDClass crdOld, CRDClass crdNew) {
            	onCrdUpdate(crdOld, crdNew);
            }

            @Override
            public void onDelete(CRDClass crd, boolean deletedFinalStateUnknown) {
            	onCrdDelete(crd, deletedFinalStateUnknown);
            }
        });
    }
	
}
