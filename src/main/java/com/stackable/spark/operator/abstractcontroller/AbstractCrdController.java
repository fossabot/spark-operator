package com.stackable.spark.operator.abstractcontroller;

import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.abstractcontroller.crd.CrdClassDoneable;
import com.stackable.spark.operator.abstractcontroller.crd.CrdClassList;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.CustomResource;
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
 * @param <Crd> - pojo extending CustomResource
 */
public abstract class AbstractCrdController<Crd extends CustomResource> implements Runnable {
    private static final Logger logger = Logger.getLogger(AbstractCrdController.class.getName());
	
    private static final Integer WORKING_QUEUE_SIZE	= 1024;
	
    protected BlockingQueue<String> blockingQueue;
    protected List<HasMetadata> crdMetadata;
	protected String namespace;
	protected String crdPath;
    
	protected KubernetesClient client;
	protected SharedInformerFactory informerFactory;
	
	protected SharedIndexInformer<Crd> crdSharedIndexInformer;
	protected Lister<Crd> crdLister;
	
    protected MixedOperation<Crd,CrdClassList<Crd>,CrdClassDoneable<Crd>,Resource<Crd, CrdClassDoneable<Crd>>> crdClient; 
	
	@SuppressWarnings("unchecked")
	public AbstractCrdController(KubernetesClient client, String crdPath, Long resyncCycle) {
		this.client = client;
		
		if(this.client == null) {
			this.client = new DefaultKubernetesClient();
		}
		
        this.namespace = this.client.getNamespace();
        
        if (this.namespace == null) {
        	this.namespace = "default";
            logger.trace("No namespace found via config, assuming " + namespace);
        }
        
        this.crdMetadata = loadYaml(crdPath);

        this.blockingQueue = new ArrayBlockingQueue<>(WORKING_QUEUE_SIZE);

        this.informerFactory = this.client.informers();
        
        CustomResourceDefinitionContext context = getCrdContext(crdMetadata);
        
		this.crdSharedIndexInformer = informerFactory.sharedIndexInformerForCustomResource(
			context, 
        	(Class<Crd>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], 
        	CrdClassList.class, 
        	resyncCycle
        ); 
		
		this.crdLister = new Lister<>(crdSharedIndexInformer.getIndexer(), namespace);
		
		this.crdClient = this.client.customResources(
			context,
	        (Class<Crd>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], 
	        (Class<CrdClassList<Crd>>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0],
	        (Class<CrdClassDoneable<Crd>>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]
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
    	
		CustomResourceDefinition crd = (CustomResourceDefinition)metaData.get(0);

		// check spec
		if(crd.getSpec().getVersions().isEmpty()) {
			logger.warn("No versions available - return null");
    		return null;
		} 
		else if(crd.getSpec().getVersions().size() > 1) {
			logger.warn("multiple versions available ... using first one");
		}
		
    	builder.withVersion(crd.getSpec().getVersions().get(0).getName());
    	builder.withKind(crd.getSpec().getNames().getKind());
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
    protected abstract void process(Crd crd);
 
    /**
     * Overwrite method to add more informers to be synced (e.g. pods)
     */
    protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced());
    }
    
    /**
     * Load yaml CRD from file path
     * @param path - path to yaml file
     * @return List<HasMetadata> of that CRD
     */
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
                // Get the crds resource's name from key which is in format namespace/name
                Crd crd = crdLister.get(key.split("/")[1]);

                if (crd != null) {
                    process(crd);
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    protected void onCrdAdd(Crd crd) {
    	enqueueCrd(crd);
    }
    
    protected void onCrdUpdate(Crd crdOld, Crd crdNew) {
    	enqueueCrd(crdNew);
    }
    
    protected void onCrdDelete(Crd crd, boolean deletedFinalStateUnknown) {
    	// noop
    }
    
    /**
     * Register crd event handlers for add, update and delete
     */
    private void registerCrdEventHandler() {
        this.crdSharedIndexInformer.addEventHandler(new ResourceEventHandler<Crd>() {
            @Override
            public void onAdd(Crd crd) {
            	logger.trace("onAdd: " + crd);
            	onCrdAdd(crd);
            }
            @Override
            public void onUpdate(Crd crdOld, Crd crdNew) {
            	logger.trace("onUpdate:\ncrdOld: " + crdOld + "\ncrdNew: " + crdNew);
            	onCrdUpdate(crdOld, crdNew);
            }
            @Override
            public void onDelete(Crd crd, boolean deletedFinalStateUnknown) {
            	logger.trace("onDelete: " + crd);
            	onCrdDelete(crd, deletedFinalStateUnknown);
            }
        });
    }
    
    /**
     * Add elements / events to the blocking queue
     * @param crd - your CRD class
     */
    protected void enqueueCrd(Crd crd) {
        String key = Cache.metaNamespaceKeyFunc(crd);
        if (key != null && !key.isEmpty()) {
            blockingQueue.add(key);
        }
    }
    
    /**
     * Return mixed operation for this crd controller
     * @return MixedOperation for specified controller
     */
	public MixedOperation<Crd, CrdClassList<Crd>, CrdClassDoneable<Crd>, Resource<Crd, CrdClassDoneable<Crd>>> getCrdClient() {
		return crdClient;
	}
	
	/**
	 * Return mixed operation for another custom crd controller
	 * @param <T> CrdClass extending CustomResource
	 * @param crdPath - path to yaml specification
	 * @return MixedOperation for specified controller
	 */
	@SuppressWarnings("unchecked")
	public <T extends CustomResource> MixedOperation
	<T, CrdClassList<T>, CrdClassDoneable<T>, Resource<T, CrdClassDoneable<T>>> 
		getCustomCrdClient(String crdPath, T t) {

		// get cluster crd meta data
		List<HasMetadata> clusterMetaData = loadYaml(crdPath);
		CustomResourceDefinitionContext context = getCrdContext(clusterMetaData);
		
		return client.customResources(
			context,
			t.getClass(),
			(new CrdClassList<T>()).getClass(), 
			(new CrdClassDoneable<T>(t, null)).getClass()
		); 
	}
    
	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	
	public KubernetesClient getClient() {
		return client;
	}
	
	public void setClient(KubernetesClient client) {
		this.client = client;
	}

}
