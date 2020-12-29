package tech.stackable.spark.operator.application.crd;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize
public class SparkApplicationSpec implements KubernetesResource {

  private static final long serialVersionUID = 7517905868632922670L;

  private String image;
  private String mainApplicationFile;
  private String mainClass;
  private String sparkConfigMap;
  private String type;
  private String mode;
  private String restartPolicy;
  private String imagePullPolicy;
  private String secret;
  private SparkApplicationDriver driver;
  private SparkApplicationExecutor executor;
  private List<String> dependencies = new ArrayList<>();
  private List<String> args = new ArrayList<>();
  private List<EnvVar> sparkConfiguration = new ArrayList<>();
  private List<EnvVar> env = new ArrayList<>();

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public String getMainApplicationFile() {
    return mainApplicationFile;
  }

  public void setMainApplicationFile(String mainApplicationFile) {
    this.mainApplicationFile = mainApplicationFile;
  }

  public String getMainClass() {
    return mainClass;
  }

  public void setMainClass(String mainClass) {
    this.mainClass = mainClass;
  }

  public String getSparkConfigMap() {
    return sparkConfigMap;
  }

  public void setSparkConfigMap(String sparkConfigMap) {
    this.sparkConfigMap = sparkConfigMap;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getMode() {
    return mode;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public String getRestartPolicy() {
    return restartPolicy;
  }

  public void setRestartPolicy(String restartPolicy) {
    this.restartPolicy = restartPolicy;
  }

  public String getImagePullPolicy() {
    return imagePullPolicy;
  }

  public void setImagePullPolicy(String imagePullPolicy) {
    this.imagePullPolicy = imagePullPolicy;
  }

  public String getSecret() {
    return secret;
  }

  public void setSecret(String secret) {
    this.secret = secret;
  }

  public SparkApplicationDriver getDriver() {
    return driver;
  }

  public void setDriver(SparkApplicationDriver driver) {
    this.driver = driver;
  }

  public SparkApplicationExecutor getExecutor() {
    return executor;
  }

  public void setExecutor(SparkApplicationExecutor executor) {
    this.executor = executor;
  }

  public List<String> getDependencies() {
    return dependencies;
  }

  public void setDependencies(List<String> dependencies) {
    this.dependencies = dependencies;
  }

  public List<String> getArgs() {
    return args;
  }

  public void setArgs(List<String> args) {
    this.args = args;
  }

  public List<EnvVar> getSparkConfiguration() {
    return sparkConfiguration;
  }

  public void setSparkConfiguration(List<EnvVar> sparkConfiguration) {
    this.sparkConfiguration = sparkConfiguration;
  }

  public List<EnvVar> getEnv() {
    return env;
  }

  public void setEnv(List<EnvVar> env) {
    this.env = env;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((args == null) ? 0 : args.hashCode());
    result = prime * result + ((dependencies == null) ? 0 : dependencies.hashCode());
    result = prime * result + ((driver == null) ? 0 : driver.hashCode());
    result = prime * result + ((env == null) ? 0 : env.hashCode());
    result = prime * result + ((executor == null) ? 0 : executor.hashCode());
    result = prime * result + ((image == null) ? 0 : image.hashCode());
    result = prime * result + ((imagePullPolicy == null) ? 0 : imagePullPolicy.hashCode());
    result = prime * result + ((mainApplicationFile == null) ? 0 : mainApplicationFile.hashCode());
    result = prime * result + ((mainClass == null) ? 0 : mainClass.hashCode());
    result = prime * result + ((mode == null) ? 0 : mode.hashCode());
    result = prime * result + ((restartPolicy == null) ? 0 : restartPolicy.hashCode());
    result = prime * result + ((sparkConfigMap == null) ? 0 : sparkConfigMap.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SparkApplicationSpec other = (SparkApplicationSpec) obj;
    if (args == null) {
      if (other.args != null) {
        return false;
      }
    } else if (!args.equals(other.args)) {
      return false;
    }
    if (dependencies == null) {
      if (other.dependencies != null) {
        return false;
      }
    } else if (!dependencies.equals(other.dependencies)) {
      return false;
    }
    if (driver == null) {
      if (other.driver != null) {
        return false;
      }
    } else if (!driver.equals(other.driver)) {
      return false;
    }
    if (env == null) {
      if (other.env != null) {
        return false;
      }
    } else if (!env.equals(other.env)) {
      return false;
    }
    if (executor == null) {
      if (other.executor != null) {
        return false;
      }
    } else if (!executor.equals(other.executor)) {
      return false;
    }
    if (image == null) {
      if (other.image != null) {
        return false;
      }
    } else if (!image.equals(other.image)) {
      return false;
    }
    if (imagePullPolicy == null) {
      if (other.imagePullPolicy != null) {
        return false;
      }
    } else if (!imagePullPolicy.equals(other.imagePullPolicy)) {
      return false;
    }
    if (mainApplicationFile == null) {
      if (other.mainApplicationFile != null) {
        return false;
      }
    } else if (!mainApplicationFile.equals(other.mainApplicationFile)) {
      return false;
    }
    if (mainClass == null) {
      if (other.mainClass != null) {
        return false;
      }
    } else if (!mainClass.equals(other.mainClass)) {
      return false;
    }
    if (mode == null) {
      if (other.mode != null) {
        return false;
      }
    } else if (!mode.equals(other.mode)) {
      return false;
    }
    if (restartPolicy == null) {
      if (other.restartPolicy != null) {
        return false;
      }
    } else if (!restartPolicy.equals(other.restartPolicy)) {
      return false;
    }
    if (sparkConfigMap == null) {
      if (other.sparkConfigMap != null) {
        return false;
      }
    } else if (!sparkConfigMap.equals(other.sparkConfigMap)) {
      return false;
    }
    if (type == null) {
      return other.type == null;
    } else {
      return type.equals(other.type);
    }
  }

}
