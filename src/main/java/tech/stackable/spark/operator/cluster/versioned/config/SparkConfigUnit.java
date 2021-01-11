package tech.stackable.spark.operator.cluster.versioned.config;

public enum SparkConfigUnit {
  /**
   * URL
   */
  URL("(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,4}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)"),
  /**
   * URI
   */
  URI("^(?:http(s)?:\\/\\/)?[\\w.-]+(?:\\.[\\w\\.-]+)+[\\w\\-\\._~:/?#[\\]@!\\$&'\\(\\)\\*\\+,;=.]+$"),
  /**
   * DIRECTORY
   */
  DIR("^/|(/[\\w-]+)+$"),
  /**
   * BINARY TODO: check for executable
   */
  BIN("^/|(/[\\w-]+)+$"),
  /**
   * FILE
   */
  FILE("^/|(/[\\w-]+)+$"),
  /**
   * EMAIL
   */
  EMAIL(""),
  /**
   * PASSWORD
   */
  PASSWORD(""),
  /**
   * DATE
   */
  DATE(""),
  /**
   * IP
   */
  IP("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"),
  /**
   * PORT
   */
  PORT("^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)"),
  /**
   * CONFIG (x=y)
   */
  CONFIG(""),
  /**
   * CLASS (x.y.z)
   */
  CLASS(""),
  /**
   * MEMORY (b,k,kb,m,mb,g,gb,t,tb ..)
   */
  MEM("(^\\p{N}+)(?:\\s*)((?:b|k|m|g|t|p|kb|mb|gb|tb|pb)\\b$)"),
  /**
   * TIME (ns,mus,ms,s,m,h,d...)
   */
  TIME("(^\\p{N}+)(?:\\s*)((?:ns|mus|ms|s|m|min|h|d)\\b$)"),
  /**
   * NUMBER (unit less amount / count)
   */
  NUMBER("^-?[0-9][0-9,\\.]+$");

  private String regex;

  SparkConfigUnit(String regex) {
    this.regex = regex;
  }

  public String getRegex() { return regex; }
}
