package com.stackable.spark.operator.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.cluster.SparkCluster;
import com.stackable.spark.operator.cluster.crd.SparkNode;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Lister;

/**
 * Mirroring ReplicaSet from Kubernetes API for master and worker
 * 
 */
public class SparkClusterController {
    public static final Logger logger = Logger.getLogger(SparkClusterController.class.getName());

    public static final String SPARK_CLUSTER_KIND 	= "SparkCluster";

    public static final String POD_RUNNING 			= "Running";
    public static final String POD_PENDING 			= "Pending";

    public static final String APP_LABEL 			= "cluster";

    public static final Integer WORKING_QUEUE_SIZE	= 1024;

    private BlockingQueue<String> workqueue;

    private SharedIndexInformer<SparkCluster> sparkClusterInformer;
    private Lister<SparkCluster> sparkClusterLister;

    private SharedIndexInformer<Pod> podInformer;
    private Lister<Pod> podLister;

    private KubernetesClient kubernetesClient;

    public SparkClusterController(KubernetesClient kubernetesClient,
    							  SharedIndexInformer<Pod> podInformer,
    							  SharedIndexInformer<SparkCluster> sparkClusterInformer,
    							  String namespace) {
        this.kubernetesClient = kubernetesClient;

        this.sparkClusterLister = new Lister<>(sparkClusterInformer.getIndexer(), namespace);
        this.sparkClusterInformer = sparkClusterInformer;

        this.podLister = new Lister<>(podInformer.getIndexer(), namespace);
        this.podInformer = podInformer;

        this.workqueue = new ArrayBlockingQueue<>(WORKING_QUEUE_SIZE);
    }

    public void create() {
        sparkClusterInformer.addEventHandler(new ResourceEventHandler<SparkCluster>() {
            @Override
            public void onAdd(SparkCluster sparkCluster) {
                enqueueSparkCluster(sparkCluster);
            }

            @Override
            public void onUpdate(SparkCluster sparkCluster, SparkCluster newSparkCluster) {
                enqueueSparkCluster(newSparkCluster);
            }

            @Override
            public void onDelete(SparkCluster sparkCluster, boolean deletedFinalStateUnknown) {
            	// skip
            }
        });

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
            public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
                // skip
            }
        });

    }

    public void run() {
        logger.info("Starting " + SparkCluster.class.getName() + " Controller");

        // wait until informer has synchronized
        while (!podInformer.hasSynced() || !sparkClusterInformer.hasSynced()) {;}

        while (true) {
            try {
                logger.info("Trying to fetch item from workqueue [# " + workqueue.size() + "]...");

                String key = workqueue.take();
                Objects.requireNonNull(key, "Key can't be null");
                logger.info(String.format("Got %s", key));

                if (key.isEmpty() || (!key.contains("/"))) {
                    logger.warn(String.format("Invalid resource key: %s", key));
                }

                // Get the SparkCluster resource's name from key which is in format namespace/name
                String name = key.split("/")[1];
                SparkCluster sparkCluster = sparkClusterLister.get(key.split("/")[1]);

                if (sparkCluster == null) {
                    logger.fatal(String.format("SparkCluster %s in workqueue no longer exists", name));
                    return;
                }

                // adapt master and workers
                reconcile(sparkCluster, sparkCluster.getSpec().getMaster());
                reconcile(sparkCluster, sparkCluster.getSpec().getWorker());
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                logger.fatal("Controller interrupted...");
            }
        }
    }

    /**
     * Reconcile the spark cluster. Compare current with desired state and adapt.
     * @param sparkCluster specified spark cluster
     */
    protected void reconcile(SparkCluster sparkCluster, SparkNode node) {
        List<String> pods = podCountByName(sparkCluster, node);

        if (pods.isEmpty()) {
            createPods(node.getInstances(), sparkCluster, node);
            return;
        }

        int existingPods = pods.size();

        // Compare with desired state (spec.master.node.instances)
        // If less then create new pods
        if (existingPods < node.getInstances()) {
            createPods(node.getInstances() - existingPods, sparkCluster, node);
        }

        // If more then delete old pods
        int diff = existingPods - node.getInstances();

        for (; diff > 0; diff--) {
        	// TODO: dont remove current master leader!
            String podName = pods.remove(0);
            kubernetesClient.pods()
            				.inNamespace(sparkCluster.getMetadata().getNamespace())
            				.withName(podName)
            				.delete();
        }
    }

    private String createPodName(SparkCluster sparkCluster, SparkNode node) {
    	return sparkCluster.getMetadata().getName() + "-" + node.getTypeName() + "-";
    }

    private void createPods(int numberOfPods, SparkCluster sparkCluster, SparkNode node) {
        for (int index = 0; index < numberOfPods; index++) {
            Pod pod = createNewPod(sparkCluster, node);
            kubernetesClient.pods().inNamespace(sparkCluster.getMetadata().getNamespace()).create(pod);
        }
    }

    private List<String> podCountByName(SparkCluster sparkCluster, SparkNode node) {
        List<String> podNames = new ArrayList<>();
        List<Pod> pods = podLister.list();
        String nodeName = createPodName(sparkCluster, node);
        for (Pod pod : pods) {
        	// filter for terminating pods
        	if(pod.getMetadata().getDeletionTimestamp() != null) {
        		logger.info("Found Terminating pod: " + pod.getMetadata().getName());
        		continue;
        	}
        	// differentiate masters and workers
        	if (pod.getMetadata().getName().contains(nodeName)) {
                if (pod.getStatus().getPhase().equals(POD_RUNNING) ||
                	pod.getStatus().getPhase().equals(POD_PENDING)) {
                    podNames.add(pod.getMetadata().getName());
                }
            }
        }

        logger.info(String.format("%s count: %d -> spec: %d", nodeName, podNames.size(), node.getInstances()));
        return podNames;
    }

	private Pod createNewPod(SparkCluster sparkCluster, SparkNode node) {
		// TODO: replace hardcoded
		ConfigMapVolumeSource cm = new ConfigMapVolumeSourceBuilder().withName("spark-cluster-worker-config").build();
		Volume vol = new VolumeBuilder().withName("spark-cluster-config").withConfigMap(cm).build();
    	
        return new PodBuilder()
                .withNewMetadata()
                  .withGenerateName(createPodName(sparkCluster, node))
                  .withNamespace(sparkCluster.getMetadata().getNamespace())
                  .withLabels(Collections.singletonMap(APP_LABEL, sparkCluster.getMetadata().getName()))
                  .addNewOwnerReference()
                  .withController(true)
                  .withKind(SPARK_CLUSTER_KIND)
                  .withApiVersion(sparkCluster.getApiVersion())
                  .withName(sparkCluster.getMetadata().getName())
                  .withNewUid(sparkCluster.getMetadata().getUid())
                  .endOwnerReference()
                .endMetadata()
                .withNewSpec()
                // TODO: check for null / zero elements
                .withNodeSelector(node.getSelectors().get(0).getSelector().getMatchLabels())
                .withVolumes(vol)
                .addNewContainer()
	            	.withName(node.getImage())
	            	.withImage(node.getImage())
	            	.withCommand(node.getCommand())
	            	.withArgs(node.getArgs())
	                  .addNewVolumeMount()
	                  	// TODO: replace hardcoded
	                  	.withMountPath("/etc/config")
	                  	.withName("spark-cluster-config")
	                  .endVolumeMount()
                .endContainer()
                .endSpec()
                .build();
    }
    
    private void enqueueSparkCluster(SparkCluster sparkCluster) {
        String key = Cache.metaNamespaceKeyFunc(sparkCluster);
        if (key != null && !key.isEmpty()) {
            logger.info("Adding item to workqueue: " + key);
            workqueue.add(key);
        }
    }

    private void handlePodObject(Pod pod) {
        OwnerReference ownerReference = getControllerOf(pod);
        Objects.requireNonNull(ownerReference);
        if (!ownerReference.getKind().equalsIgnoreCase(SPARK_CLUSTER_KIND)) {
            return;
        }
        SparkCluster sparkCluster = sparkClusterLister.get(ownerReference.getName());
        if (sparkCluster != null) {
        	enqueueSparkCluster(sparkCluster);
        }
    }

    private OwnerReference getControllerOf(Pod pod) {
        List<OwnerReference> ownerReferences = pod.getMetadata().getOwnerReferences();
        for (OwnerReference ownerReference : ownerReferences) {
            if (ownerReference.getController().equals(Boolean.TRUE)) {
                return ownerReference;
            }
        }
        return null;
    }
}
