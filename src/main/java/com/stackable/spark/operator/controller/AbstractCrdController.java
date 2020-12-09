package com.stackable.spark.operator.controller;

import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Lister;

/**
 * Abstract CRD Controller to work with customize CRDs. Applies given CRD and orders events (add, update, delete)
 * in an blocking queue. CRDClass extending CustomResource and CRDClassList extending CustomResourceList required!
 * @param <CRDClass> - pojo extending CustomResource
 * @param <CRDClassList> - list version of pojo extending CustomResourceList
 * @param <CRDClassDoneable> - doneable extending CustomResourceDoneable
 */
public abstract class AbstractCrdController<CRDClass extends CustomResource, 
											CRDClassList extends CustomResourceList<CRDClass>,
											CRDClassDoneable extends CustomResourceDoneable<CRDClass>>
											implements Runnable {
    private static final Logger logger = Logger.getLogger(AbstractCrdController.class.getName());
	
    private static final Integer WORKING_QUEUE_SIZE	= 1024;
	
    protected BlockingQueue<String> blockingQueue;
    protected List<HasMetadata> crdMetadata;
	protected String namespace;
	protected String crdPath;
    
	protected KubernetesClient client;
	protected SharedInformerFactory informerFactory;
	
	protected SharedIndexInformer<CRDClass> crdSharedIndexInformer;
	protected Lister<CRDClass> crdLister;
	
    protected MixedOperation<CRDClass,CRDClassList,CRDClassDoneable,Resource<CRDClass, CRDClassDoneable>> crdClient; 
	
	@SuppressWarnings("unchecked")
	public AbstractCrdController(String crdPath, Long resyncCycle) {
		this.client = new DefaultKubernetesClient();
        this.namespace = client.getNamespace();
        
        if (this.namespace == null) {
        	this.namespace = "default";
            //logger.debug("No namespace found via config, assuming " + namespace);
        }
        
        this.crdMetadata = loadYaml(crdPath);

        this.blockingQueue = new ArrayBlockingQueue<>(WORKING_QUEUE_SIZE);

        this.informerFactory = client.informers();
        
        CustomResourceDefinitionContext context = getCrdContext(crdMetadata);
        
		this.crdSharedIndexInformer = informerFactory.sharedIndexInformerForCustomResource(
			context, 
        	(Class<CRDClass>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], 
        	(Class<CRDClassList>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1], 
        	resyncCycle
        ); 
		
		this.crdLister = new Lister<>(crdSharedIndexInformer.getIndexer(), namespace);
		
		this.crdClient = client.customResources(
			context,
	        (Class<CRDClass>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], 
	        (Class<CRDClassList>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1],
	        (Class<CRDClassDoneable>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[2]
		); 
		
        // initialize CRD -> should be one for each controller
        createOrReplaceCRD(namespace, crdMetadata);
		
		// register events handler
		registerCrdEventHandler();
	}
	
	/**
	 * Build CRD context for the sharedIndexInformer or other operations from the CRD yaml spec
	 * @param metaData - List<HasMetadata> with the CRD(s) for the controller
	 * @return CRD context
	 */
	protected CustomResourceDefinitionContext getCrdContext(List<HasMetadata> metaData) {

    	CustomResourceDefinitionContext.Builder builder = new CustomResourceDefinitionContext.Builder();
    	// check metadata
    	if(metaData.isEmpty()) {
    		logger.warn("No metadata available - return null");
    		return null;
    	}
    	else if(metaData.size() > 1) {
        	logger.warn("multiple crds available ... using first one");
        }
    	
		CustomResourceDefinition crd =  (CustomResourceDefinition)metaData.get(0);

		// check spec
		if(crd.getSpec().getVersions().isEmpty()) {
			logger.warn("No versions available - return null");
    		return null;
		} 
		else if(crd.getSpec().getVersions().size() > 1) {
			logger.warn("multiple versions available ... using first one");
		}
		
    	builder.withVersion(crd.getSpec().getVersions().get(0).getName());
    	builder.withGroup(crd.getSpec().getGroup());
    	builder.withScope(crd.getSpec().getScope());
    	builder.withPlural(crd.getSpec().getNames().getPlural());
    	
        return builder.build();
	}
	
	/**
	 * Overwrite method to specify what should be done after the blocking queue has elements
	 * @param controller - controller class extending AbstractCrdController
	 * @param crd - specified CRD resource class for that controller
	 */
    protected abstract void process(CRDClass crd);
 
    /**
     * Overwrite method to add more informers to be synced (e.g. pods)
     */
    protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced());
		logger.info("AbstractCrdController informer initialized ... waiting for changes");
    }
    
    protected List<HasMetadata> loadYaml(String path) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
    	List<HasMetadata> result = client.load(is).get();
    	return client.resourceList(result).inNamespace(namespace).get(); 
    }
    
    /**
     * Create or replace the CRD in the API Server
     * @param namespace - given namespace
     * @param metaData - List<HasMetadata> from the yaml file
     * @return List<HasMetadata> of affected CRDs
     */
    protected List<HasMetadata> createOrReplaceCRD(String namespace, List<HasMetadata> metaData) {
    	List<HasMetadata> result = client.resourceList(metaData).inNamespace(namespace).createOrReplace(); 
    	return result;
    }
    
    /**
     * Waits until all informers are synced and waits for elements to be in the queue and runs process()
     */
    public void run() {
    	// add informers
		informerFactory.addSharedInformerEventListener(exception -> logger.fatal("Tip: missing/bad CRDs?\n" + exception));
		// start informers
		informerFactory.startAllRegisteredInformers();
		
        // wait until informers have synchronized
        waitForAllInformersSynced();
        // loop for synchronization
        while (true) {
            try {
                String key = blockingQueue.take();
                Objects.requireNonNull(key, "Key can't be null");

                if (key.isEmpty() || (!key.contains("/"))) {
                    continue;
                }
                // Get the SparkCluster resource's name from key which is in format namespace/name
                CRDClass crd = crdLister.get(key.split("/")[1]);

                if (crd == null) {
                    continue;
                }

                process(crd);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    protected void onCrdAdd(CRDClass crd) {
    	enqueueCrd(crd);
    }
    
    protected void onCrdUpdate(CRDClass crdOld, CRDClass crdNew) {
    	enqueueCrd(crdNew);
    }
    
    protected void onCrdDelete(CRDClass crd, boolean deletedFinalStateUnknown) {
    	// noop
    }
    
    private void registerCrdEventHandler() {
        this.crdSharedIndexInformer.addEventHandler(new ResourceEventHandler<CRDClass>() {
            @Override
            public void onAdd(CRDClass crd) {
            	logger.trace("onAdd: " + crd);
            	onCrdAdd(crd);
            }
            @Override
            public void onUpdate(CRDClass crdOld, CRDClass crdNew) {
            	logger.trace("onUpdate:\ncrdOld: " + crdOld + "\ncrdNew: " + crdNew);
            	onCrdUpdate(crdOld, crdNew);
            }
            @Override
            public void onDelete(CRDClass crd, boolean deletedFinalStateUnknown) {
            	logger.trace("onDelete: " + crd);
            	onCrdDelete(crd, deletedFinalStateUnknown);
            }
        });
    }
    
    /**
     * Add elements / events to the blocking queue
     * @param crd - your CRD class
     */
    protected void enqueueCrd(CRDClass crd) {
        String key = Cache.metaNamespaceKeyFunc(crd);
        if (key != null && !key.isEmpty()) {
            blockingQueue.add(key);
        }
    }
    
	public KubernetesClient getClient() {
		return client;
	}

	public void setClient(KubernetesClient client) {
		this.client = client;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public SharedIndexInformer<CRDClass> getCrdSharedIndexInformer() {
		return crdSharedIndexInformer;
	}

	public Lister<CRDClass> getCrdLister() {
		return crdLister;
	}

	public MixedOperation<CRDClass, CRDClassList, CRDClassDoneable, Resource<CRDClass, CRDClassDoneable>> getCrdClient() {
		return crdClient;
	}

}
