package com.stackable.spark.operator.controller;

import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Lister;

/**
 * Abstract CRD Controller to work with customize CRDs. Applies given CRD and orders events (add, update, delete)
 * in an blocking queue. CRDClass extending CustomResource and CRDClassList extending CustomResourceList required!
 * @param <CRDClass>
 * @param <CRDClassList>
 */
public abstract class AbstractCrdController<CRDClass extends CustomResource, 
											CRDClassList extends CustomResourceList<CRDClass>> {
	
    public static final Integer WORKING_QUEUE_SIZE	= 1024;
	
    protected BlockingQueue<String> blockingQueue;
	
	protected KubernetesClient client;
	protected String namespace;
	
	protected SharedIndexInformer<CRDClass> crdSharedIndexInformer;
	protected Lister<CRDClass> crdLister;
	
	protected String crdPath;
	
	@SuppressWarnings("unchecked")
	public AbstractCrdController(
		KubernetesClient client,
		SharedInformerFactory informerFactory,
		CustomResourceDefinitionContext crdContext,
		String namespace,
		String crdPath,
		Long resyncCycle) {
		
		this.client = client;
		this.namespace = namespace;
        this.blockingQueue = new ArrayBlockingQueue<>(WORKING_QUEUE_SIZE);

		this.crdSharedIndexInformer = informerFactory.sharedIndexInformerForCustomResource(
			crdContext, 
        	(Class<CRDClass>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], 
        	(Class<CRDClassList>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1], 
        	resyncCycle
        ); 
		
		this.crdLister = new Lister<>(crdSharedIndexInformer.getIndexer(), namespace);
		
        // initialize CRD -> should be one for each controller
        createOrReplaceCRD(namespace, crdPath);
		
		// register events handler
		registerCrdEventHandler();
	}
	
	/**
	 * Overwrite method to specify what should be done after the blocking queue has elements
	 * @param controller - controller class extending AbstractCrdController
	 * @param crd - specified CRD resource class for that controller
	 */
    protected abstract void process(AbstractCrdController<CRDClass,CRDClassList> controller, CRDClass crd);
 
    /**
     * Overwrite method to add more informers to be synced (e.g. pods)
     */
    protected void waitForAllInformersSynced() {
    	while (!crdSharedIndexInformer.hasSynced());
    }
    
    /**
     * Waits until all informers are synced and waits for elements to be in the queue and runs process()
     */
    public void start() {
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
                    return;
                }

                process(this, crd);
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
    
    protected void onCrdDelete(CRDClass crd, boolean deletedFinalStateUnknown) {}
    
    private void registerCrdEventHandler() {
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
    
    /**
     * Create or replace the CRD in the API Server
     * @param namespace - given namespace
     * @param path - path to the CRD yaml file
     * @return
     */
    protected List<HasMetadata> createOrReplaceCRD(String namespace, String path) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
    	List<HasMetadata> result = client.load(is).get();
    	client.resourceList(result).inNamespace(namespace).createOrReplace(); 
    	return result;
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

	public void setCrdSharedIndexInformer(SharedIndexInformer<CRDClass> crdSharedIndexInformer) {
		this.crdSharedIndexInformer = crdSharedIndexInformer;
	}

	public Lister<CRDClass> getCrdLister() {
		return crdLister;
	}

	public void setCrdLister(Lister<CRDClass> crdLister) {
		this.crdLister = crdLister;
	}

}
