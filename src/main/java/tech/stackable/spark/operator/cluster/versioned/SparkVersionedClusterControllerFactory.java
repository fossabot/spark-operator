package tech.stackable.spark.operator.cluster.versioned;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;

public final class SparkVersionedClusterControllerFactory {

  private SparkVersionedClusterControllerFactory() {}

  public static SparkVersionedClusterController getSparkVersionedController(String image, KubernetesClient client, Lister<Pod> podLister, Lister<SparkCluster> crdLister,
    MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> crdClient) {

    SparkVersionedClusterController controller = null;
    SparkSupportedVersion version = SparkSupportedVersion.getVersionType(image, 0);

    switch(version) {
      case V_2_4_7:
        controller = new SparkVersionedClusterControllerV247(client, podLister, crdLister, crdClient);
        break;
      case V_3_0_1:
        controller = new SparkVersionedClusterControllerV301(client, podLister, crdLister, crdClient);
        break;
      case NOT_SUPPORTED:
      default:
        break;
    }

    return controller;
  }

  public enum SparkSupportedVersion {
    NOT_SUPPORTED(0,0,0),
    V_2_4_7(2,4,7),
    V_3_0_1(3,0,1);

    /**
     * major releases may differ in APIs etc. e.g. 2.0.0 -> 3.0.0
     */
    private final int major;
    /**
     * minor releases may adapt config parameters or even API but should be compatible with previous versions from major e.g. 2.0.0 -> 2.1.0
     */
    private final int minor;
    /**
     * maintenance releases are for bug fixes etc. no incompatible changes to be expected e.g. 2.0.0 -> 2.0.1
     */
    private final int maintenance;

    SparkSupportedVersion(int major, int minor, int maintenance) {
      this.major = major;
      this.minor = minor;
      this.maintenance = maintenance;
    }

    public int getMajor() {
      return major;
    }

    public int getMinor() {
      return minor;
    }

    public int getMaintenance() {
      return maintenance;
    }

    /**
     * Check if given image version can be handled by any available version handler
     *
     * @param imageVersion version given via crd
     * @param matchLevel level of matching: 0 -> only major; 1 -> major and minor; 2 -> major, minor, maintenance
     * @return respective version handler or not supported
     */
    public static SparkSupportedVersion getVersionType(String imageVersion, int matchLevel) {
      // assume format -> spark:major.minor.maintenance
      String regex = "\\d.\\d.\\d$";

      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(imageVersion);

      if(!matcher.find()) {
        return NOT_SUPPORTED;
      }

      String v = matcher.group();
      String[] split = v.split("\\.");

      for(SparkSupportedVersion supportedVersion : values()) {
        int[] version = {supportedVersion.getMajor(), supportedVersion.getMinor(), supportedVersion.getMaintenance()};
        boolean match = true;

        for(int i = 0; i <= matchLevel; i++) {
          if(Integer.parseInt(split[i]) != version[i]) {
            match = false;
          }
        }
        // found version match
        if(match) {
          return supportedVersion;
        }
      }

      return NOT_SUPPORTED;
    }
  }
}
