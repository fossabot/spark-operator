package com.stackable.spark.operator.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.SparkClusterDoneable;
import com.stackable.spark.operator.cluster.SparkClusterList;
import com.stackable.spark.operator.cluster.SparkClusterState;
import com.stackable.spark.operator.cluster.crd.SparkNode;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Lister;

/**
 * The SparkClusterController is responsible for installing the spark master and workers.
 * Scale up and down to the instances required in the specification.
 */
public class SparkClusterController extends AbstractCrdController<SparkCluster, SparkClusterList, SparkClusterDoneable> {
	
    public static final Logger logger = Logger.getLogger(SparkClusterController.class.getName());

    public static final String SPARK_CLUSTER_KIND 	= "SparkCluster";
    public static final String APP_LABEL 			= "cluster";

    private SharedIndexInformer<Pod> podInformer;
    private Lister<Pod> podLister;
    
    private SparkClusterState clusterState;
    // podName:hostname
    private Map<String,String> hostNameMap = new HashMap<String,String>();
    
	public SparkClusterController(
		KubernetesClient client,
		SharedInformerFactory informerFactory,
		String namespace,
		String crdPath,
		Long resyncCycle) {

		super(client, informerFactory, namespace, crdPath, resyncCycle);
		
        this.podInformer = informerFactory.sharedIndexInformerFor(Pod.class, PodList.class, resyncCycle);
        this.podLister = new Lister<>(podInformer.getIndexer(), namespace);

        this.clusterState = SparkClusterState.INITIAL;
        // register pods
        registerPodEventHandler();
    }
	
    protected CustomResourceDefinitionContext getCrdContext() {
        return new CustomResourceDefinitionContext.Builder()
            .withVersion("v1")
            .withScope("Namespaced")
            .withGroup("spark.stackable.de")
            .withPlural("sparkclusters")
            .build();
    }
	
	/**
	 * Register event handler for kubernetes pods
	 */
	private void registerPodEventHandler() {
		podInformer.addEventHandler(new ResourceEventHandler<Pod>() {
            @Override
            public void onAdd(Pod pod) {
                handlePodObject(pod);
            }
            @Override
            public void onUpdate(Pod oldPod, Pod newPod) {
                if (oldPod.getMetadata().getResourceVersion().equals(newPod.getMetadata().getResourceVersion())) {
                    return;
                }
                handlePodObject(newPod);
            }
            @Override
            public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {}
        });
	}

	@Override
	protected void process(AbstractCrdController<SparkCluster,SparkClusterList,SparkClusterDoneable> controller, SparkCluster cluster) {
		clusterState = clusterState.process(this, cluster);
	}
	
	@Override
	protected void waitForAllInformersSynced() {
		while (!crdSharedIndexInformer.hasSynced() || !podInformer.hasSynced());
		logger.info("SparkCluster informer and pod informer initialized ... waiting for changes");
	}
    
	/**
	 * Create pods with regard to spec and current state
	 * @param pods - list of available pods belonging to the given node
	 * @param cluster - current spark cluster
	 * @param node - master or worker node
	 * @return list of created pods
	 */
    public List<Pod> createPods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    	List<Pod> createdPods = new ArrayList<Pod>();
        // Compare with desired state (spec.master.node.instances)
        // If less then create new pods
        if (pods.size() < node.getInstances()) {
            for (int index = 0; index < node.getInstances() - pods.size(); index++) {
                Pod pod = client.pods().inNamespace(cluster.getMetadata().getNamespace()).create(createNewPod(cluster, node));
                createdPods.add(pod);
            }
        }
        return createdPods;
    }
    
	/**
	 * Delete pods with regard to spec and current state
	 * @param pods - list of available pods belonging to the given node
	 * @param cluster - current spark cluster
	 * @param node - master or worker node
	 * @return list of deleted pods
	 */
    public List<Pod> deletePods(List<Pod> pods, SparkCluster cluster, SparkNode node) {
    	List<Pod> deletedPods = new ArrayList<Pod>();
        // If more pods than spec delete old pods
        int diff = pods.size() - node.getInstances();

        for (; diff > 0; diff--) {
        	// TODO: dont remove current master leader!
            Pod pod = pods.remove(0);
            client.pods()
            	.inNamespace(cluster.getMetadata().getNamespace())
            	.withName(pod.getMetadata().getName())
            	.delete();
            deletedPods.add(pod);
        }
        return deletedPods;
    }
    
    /**
     * Return number of pods for given nodes - Terminating is excluded
     * @param cluster - current spark cluster
     * @param nodes - master or worker node
     * @return list of pods belonging to the given node
     */
    public List<Pod> getPodsByNode(SparkCluster cluster, SparkNode... nodes) {
        List<Pod> podList = new ArrayList<>();
    	
    	for(SparkNode node : nodes) {
	        String nodeName = createPodName(cluster, node);
	        
	        for (Pod pod : podLister.list()) {
	        	// filter for terminating pods
	        	if(pod.getMetadata().getDeletionTimestamp() != null) {
	        		continue;
	        	}
	        	if (pod.getMetadata().getName().contains(nodeName)) {
	        		// TODO: Filter PodStatus: Running...Failure etc.
                    podList.add(pod);
	            }
	        }
    	}
        return podList;
    }
    
    /**
     * Create pod name schema
     * @param cluster - current spark cluster
     * @param node - master or worker node
     * @return pod name
     */
    private String createPodName(SparkCluster cluster, SparkNode node) {
    	return cluster.getMetadata().getName() + "-" + node.getPodTypeName() + "-";
    }

    /**
     * Check incoming pods for owner reference spark cluster and add to blocking queue
     * @param pod - kubernetes pod
     */
    private void handlePodObject(Pod pod) {
        OwnerReference ownerReference = getControllerOf(pod);
        Objects.requireNonNull(ownerReference);
        // check if pod belongs to spark cluster
        if (!ownerReference.getKind().equalsIgnoreCase(SPARK_CLUSTER_KIND)) {
            return;
        }
        
        SparkCluster cluster = crdLister.get(ownerReference.getName());
        
        if (cluster == null) {
        	return;
        }

        enqueueCrd(cluster);
	}

    /**
     * Return the owner reference of that specific pod if available
     * @param pod - fabric8 Pod
     * @return pod owner reference
     */
    private OwnerReference getControllerOf(Pod pod) {
        List<OwnerReference> ownerReferences = pod.getMetadata().getOwnerReferences();
        for (OwnerReference ownerReference : ownerReferences) {
            if (ownerReference.getController().equals(Boolean.TRUE)) {
                return ownerReference;
            }
        }
        return null;
    }
    
    public void createConfigMap(SparkCluster cluster, Pod pod) {
    	String cmName = pod.getMetadata().getGenerateName() + "cm";
    	
        Resource<ConfigMap,DoneableConfigMap> configMapResource = client
        	.configMaps()
        	.inNamespace(cluster.getMetadata().getNamespace())
        	.withName(cmName);

        // create cm entry 
        Map<String,String> data = new HashMap<String,String>();
        //addToConfig(data, "SPARK_MASTER_HOST", pod.getSpec().getNodeName());
        
        StringBuffer sb = new StringBuffer();
        for( Entry<String,String> entry : data.entrySet()) {
        	sb.append(entry.getKey() + "=" + entry.getValue() + "\n");
        }
        
        data.clear();
        data.put("spark-env.sh", sb.toString());
        
        configMapResource.createOrReplace(new ConfigMapBuilder()
        	.withNewMetadata()
            	.withName(cmName)
            .endMetadata()
            .addToData(data)
            .build());
    }
    
	private Pod createNewPod(SparkCluster cluster, SparkNode node) {
		// TODO: replace hardcoded
		String cmName = createPodName(cluster, node) + "cm";
		ConfigMapVolumeSource cms = new ConfigMapVolumeSourceBuilder().withName(cmName).build();
		Volume vol = new VolumeBuilder().withName(cmName).withConfigMap(cms).build();
		List<Toleration> tolerations = new ArrayList<Toleration>();
		tolerations.add( new TolerationBuilder().withNewEffect("NoSchedule").withKey("kubernetes.io/arch").withOperator("Equal").withValue("stackable-linux").build());
		tolerations.add( new TolerationBuilder().withNewEffect("NoExecute").withKey("kubernetes.io/arch").withOperator("Equal").withValue("stackable-linux").build());
		tolerations.add( new TolerationBuilder().withNewEffect("NoExecute").withKey("node.kubernetes.io/not-ready").withOperator("Exists").withTolerationSeconds(300L).build());
		tolerations.add( new TolerationBuilder().withNewEffect("NoSchedule").withKey("node.kubernetes.io/unreachable").withOperator("Exists").build());
		tolerations.add( new TolerationBuilder().withNewEffect("NoExecute").withKey("node.kubernetes.io/unreachable").withOperator("Exists").withTolerationSeconds(300L).build());

        return new PodBuilder()
                .withNewMetadata()
                  .withGenerateName(createPodName(cluster, node))
                  .withNamespace(cluster.getMetadata().getNamespace())
                  .withLabels(Collections.singletonMap(APP_LABEL, cluster.getMetadata().getName()))
                  .addNewOwnerReference()
                  .withController(true)
                  .withKind(cluster.getKind())
                  .withApiVersion(cluster.getApiVersion())
                  .withName(cluster.getMetadata().getName())
                  .withNewUid(cluster.getMetadata().getUid())
                  .endOwnerReference()
                .endMetadata()
                .withNewSpec()
                .withTolerations(tolerations)
                // TODO: check for null / zero elements
                .withNodeSelector(node.getSelectors().get(0).getSelector().getMatchLabels())
                .withVolumes(vol)
                .addNewContainer()
                	//TODO: no ":" etc in withName
	            	.withName("spark3")
	            	.withImage(cluster.getSpec().getImage())
	            	.withCommand(node.getCommand())
	            	.withArgs(node.getArgs())
	                .addNewVolumeMount()
	                	// TODO: replace hardcoded
	                  	.withMountPath("conf")
	                  	.withName(cmName)
	                .endVolumeMount()
	                .withEnv(node.getEnv())
                .endContainer()
                .endSpec()
                .build();
    }
    
    public void addNodeConfToEnvVariables(SparkNode node) {
    	if(node.getCpu() != null && !node.getCpu().isEmpty()) {
    		node.getEnv().add(new EnvVarBuilder().withName("SPARK_WORKER_CORES").withValue(node.getCpu()).build());
    	}
    	if(node.getMemory() != null && !node.getMemory().isEmpty()) {
    		node.getEnv().add(new EnvVarBuilder().withName("SPARK_WORKER_MEMORY").withValue(node.getMemory()).build());
    	}
    }

    public Map<String, String> getHostNameMap() {
		return hostNameMap;
	}

    public void addToHostMap(String podName, String hostName) {
    	hostNameMap.put(podName, hostName);
    }
    
}
