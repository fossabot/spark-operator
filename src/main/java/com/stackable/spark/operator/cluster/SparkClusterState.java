package com.stackable.spark.operator.cluster;

import com.stackable.spark.operator.cluster.crd.SparkNode;

public enum SparkClusterState {
	
    WAIT_FOR_SPARK_CLUSTER {
        @Override
        SparkClusterState process(SparkCluster cluster, SparkNode node) {
            return CREATE_SPARK_MASTER;
        }
    },
    
    CREATE_SPARK_MASTER {
        @Override
        SparkClusterState process(SparkCluster cluster, SparkNode node) {
            return WAIT_FOR_SPARK_MASTER_HOST;
        }
    },
    
    WAIT_FOR_SPARK_MASTER_HOST {
        @Override
        SparkClusterState process(SparkCluster cluster, SparkNode node) {
            return CREATE_SPARK_WORKER;
        }
    },
    
    CREATE_SPARK_WORKER {
        @Override
        SparkClusterState process(SparkCluster cluster, SparkNode node) {
            return RECONCILE_CLUSTER;
        }
    },
    
    RECONCILE_CLUSTER {
		@Override
		SparkClusterState process(SparkCluster cluster, SparkNode node) {
			return RECONCILE_CLUSTER;
		}
    };
    
    abstract SparkClusterState process(SparkCluster cluster, SparkNode node);
}
