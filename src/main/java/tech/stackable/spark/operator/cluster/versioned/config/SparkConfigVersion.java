package tech.stackable.spark.operator.cluster.versioned.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to represent a common spark versions in the format major.minor.patch
 */
public class SparkConfigVersion {
  /**
   * major releases which may differ in APIs etc. e.g. 2.0.0 -> 3.0.0
   */
  private final int major;
  /**
   * minor releases may add / adapt config parameters or new API calls but should be compatible with previous versions from major e.g. 2.0.0 -> 2.1.0
   */
  private final int minor;
  /**
   * patch releases are for bug fixes etc. no incompatible changes to be expected e.g. 2.0.0 -> 2.0.1
   */
  private final int patch;
  /**
   * regex to match versions in the format of *major.minor.patch
   */
  private static final String REGEX = "(?<major>\\d+).(?<minor>\\d+).(?<patch>\\d+)$";

  public SparkConfigVersion(String version) {
    // Create a Pattern object
    Pattern r = Pattern.compile(REGEX);
    // Now create matcher object.
    Matcher m = r.matcher(version);

    // TODO: error handling
    if(m.find()) {
      major = Integer.parseInt(m.group("major"));
      minor = Integer.parseInt(m.group("minor"));
      patch = Integer.parseInt(m.group("patch"));
    }
    else {
      major = -1;
      minor = -1;
      patch = -1;
    }

  }

  public SparkConfigVersion(int major, int minor, int patch) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public int getPatch() {
    return patch;
  }

}
