package tech.stackable.spark.operator.cluster.versioned.config;

import java.util.List;
import java.util.stream.Collectors;

public class SparkConfigProperty {
  /**
   * single components (a,b,c...) of a config or environment variable (a.b.c... / A_B_C...)
   */
  private List<String> property;
  /**
   * default config value if not set
   */
  private String defaultValue;
  /**
   * type of config property (e.g. directory, array etc)
   */
  private SparkConfigType type;
  /**
   * unit of config property if available
   */
  private SparkConfigUnit unit;
  /**
   * indicates in which spark version property was introduced
   */
  private SparkConfigVersion asOfVersion;
  /**
   * indicates in which spark version property was deprecated
   */
  private SparkConfigVersion deprecatedAsOfVersion;
  /**
   * indicates which spark version property replaced the deprecated one if available
   */
  private String deprecatedFor;
  /**
   * nullable, optional, required
   */
  private String importance;
  /**
   * spark mode (standalone, yarn, hadoop, mesos) to be utilized in
   */
  private String mode;
  /**
   * additional documents referenced in description
   */
  private String additionalDocs;
  /**
   * property description
   */
  private String description;


  /**
   * Validate entry via unit regex, check if default values etc match type or are in min / max range
   * @return true if validated correctly, else false
   */
  public boolean validate() {
    return false;
  }

  private String getConfigName() {
    List<String> lower = property.stream()
        .map(String::toLowerCase)
        .collect(Collectors.toList());
    return String.join(".", lower);
  }

  private String getEnvName() {
    List<String> upper = property.stream()
        .map(String::toLowerCase)
        .collect(Collectors.toList());
    return String.join("_", upper);
  }
}
