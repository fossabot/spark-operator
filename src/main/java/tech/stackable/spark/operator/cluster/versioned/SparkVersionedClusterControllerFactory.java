package tech.stackable.spark.operator.cluster.versioned;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import tech.stackable.spark.operator.cluster.crd.SparkCluster;
import tech.stackable.spark.operator.cluster.versioned.config.SparkConfigVersion;
import tech.stackable.spark.operator.common.fabric8.SparkClusterDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkClusterList;

public final class SparkVersionedClusterControllerFactory {

  private SparkVersionedClusterControllerFactory() {}

  public static SparkVersionedClusterController getSparkVersionedController(
      String image,
      SparkVersionMatchLevel matchLevel,
      KubernetesClient client,
      Lister<Pod> podLister, Lister
      <SparkCluster> crdLister,
      MixedOperation<SparkCluster, SparkClusterList, SparkClusterDoneable, Resource<SparkCluster, SparkClusterDoneable>> crdClient) {

    SparkVersionedClusterController controller = null;
    SparkSupportedVersion version = SparkSupportedVersion.matchVersion(image, matchLevel);

    switch(version) {
      case V_2_4_7:
        controller = new SparkVersionedClusterControllerV247(version.getVersion(), client, podLister, crdLister, crdClient);
        break;
      case V_3_0_1:
        controller = new SparkVersionedClusterControllerV301(version.getVersion(), client, podLister, crdLister, crdClient);
        break;
      case NOT_SUPPORTED:
      default:
        break;
    }

    return controller;
  }

  /**
   * Enum to define versions with similar configurations to retrieve a matching versioned controller
   * e.g. 2.4.0 may be used for 2.4.0 up to 2.4.7
   */
  public enum SparkSupportedVersion {
    NOT_SUPPORTED(new SparkConfigVersion(-1, -1, -1)),
    V_2_4_7(new SparkConfigVersion(2, 4, 7)),
    V_3_0_1(new SparkConfigVersion(3, 0, 1));

    private final SparkConfigVersion version;

    SparkSupportedVersion(SparkConfigVersion version) {
      this.version = version;
    }

    public SparkConfigVersion getVersion() {
      return version;
    }

    /**
     * Check if given image version can be handled by any available version handler
     *
     * @param imageVersion version given via crd
     * @param versionMatchLevel level of matching
     * @return respective version handler or not supported
     */
    public static SparkSupportedVersion matchVersion(String imageVersion, SparkVersionMatchLevel versionMatchLevel) {
      SparkConfigVersion configVersion = new SparkConfigVersion(imageVersion);
      int[] toCompareVersion = {configVersion.getMajor(), configVersion.getMinor(), configVersion.getPatch()};

      for(SparkSupportedVersion supportedVersion : values()) {
        int[] version = {supportedVersion.getVersion().getMajor(), supportedVersion.getVersion().getMinor(), supportedVersion.getVersion().getPatch()};
        boolean match = true;

        for(int i = 0; i <= versionMatchLevel.getLevel(); i++) {
          if(version[i] != toCompareVersion[i]) {
            match = false;
            break;
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

  /**
   * Indicates matching level for spark version e.g.
   * - major matches x.*.*
   * - minor matches x.y.*
   * - patch matches x.y.z
   * where "*" is wildcard
   */
  public enum SparkVersionMatchLevel {
    /**
     * MAJOR e.g. 2.X.X or 3.X.X)
     */
    MAJOR(0),
    /**
     * MINOR e.g. 2.2.X or 2.4.X)
     */
    MINOR(1),
    /**
     * PATCH e.g. 2.4.6 or 2.4.7)
     */
    PATCH(2);

    private final int level;

    SparkVersionMatchLevel(int level) {
      this.level = level;
    }

    public int getLevel() {
      return level;
    }
  }
}
