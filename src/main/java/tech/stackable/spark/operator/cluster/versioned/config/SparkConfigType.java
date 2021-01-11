package tech.stackable.spark.operator.cluster.versioned.config;

import java.util.List;

/**
 * Config parameter type:
 * contains the type (String,Boolean...), minimum and maximum, given default value and a list of allowed values
 */
public class SparkConfigType {
  /**
   * type of value parameter (e.g. String, Boolean, Integer ...)
   */
  private final ConfigType type;
  /**
   * minimum for possible value
   */
  private final String min;
  /**
   * maximum for possible value
   */
  private final String max;
  /**
   * default value to be used if e.g. only a number is supplied (100 for 100ms)
   */
  private final String defaultTypeValue;
  /**
   * if only allowed values are excepted (e.g. deploy mode: client/cluster)
   */
  private final List<String> allowedValues;

  public SparkConfigType(ConfigType type, String min, String max, String defaultTypeValue, List<String> allowedValues) {
    this.type = type;
    this.min = min;
    this.max = max;
    this.defaultTypeValue = defaultTypeValue;
    this.allowedValues = allowedValues;
  }

  public ConfigType getType() {
    return type;
  }

  public String getMin() {
    return min;
  }

  public String getMax() {
    return max;
  }

  public String getDefaultTypeValue() {
    return defaultTypeValue;
  }

  public List<String> getAllowedValues() {
    return allowedValues;
  }

  /**
   * Definitions for min and max values for required types for validation
   */
  public enum ConfigType {
    STRING(0, Integer.MAX_VALUE),
    BOOLEAN(Boolean.FALSE, Boolean.TRUE),
    INTEGER(Integer.MIN_VALUE, Integer.MAX_VALUE),
    LONG(Long.MIN_VALUE, Long.MAX_VALUE),
    FLOAT(Float.MIN_VALUE, Float.MAX_VALUE),
    DOUBLE(Double.MIN_VALUE, Double.MAX_VALUE),
    ARRAY(0, Integer.MAX_VALUE);

    private final Object min;
    private final Object max;

    ConfigType(Object min, Object max) {
      this.min = min;
      this.max = max;
    }

    public Object getMin() {
      return min;
    }

    public Object getMax() {
      return max;
    }
  }
}
