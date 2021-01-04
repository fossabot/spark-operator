package tech.stackable.spark.operator.common.fabric8;

import io.fabric8.kubernetes.client.CustomResourceList;
import tech.stackable.spark.operator.cluster.manager.crd.SparkManager;

public class SparkSystemdList extends CustomResourceList<SparkManager> {

  private static final long serialVersionUID = 1649632799400856053L;
}
