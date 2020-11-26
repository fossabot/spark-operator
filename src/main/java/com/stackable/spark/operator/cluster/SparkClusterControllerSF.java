package com.stackable.spark.operator.cluster;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.stackable.spark.operator.cluster.crd.SparkNode;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;

public class SparkClusterControllerSF {
    private SparkClusterState state;

    public static final Logger logger = Logger.getLogger(SparkClusterControllerSF.class.getName());

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

    private KubernetesClient client;

    public SparkClusterControllerSF(KubernetesClient client,
    							  SharedIndexInformer<Pod> podInformer,
    							  SharedIndexInformer<SparkCluster> sparkClusterInformer,
    							  String namespace) {
        this.client = client;

        this.sparkClusterLister = new Lister<>(sparkClusterInformer.getIndexer(), namespace);
        this.sparkClusterInformer = sparkClusterInformer;

        this.podLister = new Lister<>(podInformer.getIndexer(), namespace);
        this.podInformer = podInformer;

        this.workqueue = new ArrayBlockingQueue<>(WORKING_QUEUE_SIZE);
    }
    
    public SparkClusterControllerSF() {
        state = SparkClusterState.WAIT_FOR_SPARK_CLUSTER;
    }

    public void performRequest(SparkCluster cluster, SparkNode node) {
        state = state.process(cluster, node);
    }
    
    
}
