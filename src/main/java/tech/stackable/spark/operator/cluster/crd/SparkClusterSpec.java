package tech.stackable.spark.operator.cluster.crd;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.Toleration;

@JsonDeserialize
public class SparkClusterSpec implements KubernetesResource {

  private static final long serialVersionUID = -4949229889562573739L;

  private SparkNodeMaster master;
  private SparkNodeWorker worker;
  private SparkNodeHistoryServer historyServer;
  private String image;
  private Boolean metrics;
  private String secret;
  private List<Toleration> tolerations = new ArrayList<>();

  public SparkNodeMaster getMaster() {
    return master;
  }

  public void setMaster(SparkNodeMaster master) {
    this.master = master;
  }

  public SparkNodeWorker getWorker() {
    return worker;
  }

  public void setWorker(SparkNodeWorker worker) {
    this.worker = worker;
  }

  public SparkNodeHistoryServer getHistoryServer() { return historyServer; }

  public void setHistoryServer(SparkNodeHistoryServer historyServer) { this.historyServer = historyServer; }

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public Boolean getMetrics() {
    return metrics;
  }

  public void setMetrics(Boolean metrics) {
    this.metrics = metrics;
  }

  public String getSecret() {
    return secret;
  }

  public void setSecret(String secret) {
    this.secret = secret;
  }

  public List<Toleration> getTolerations() {
    return tolerations;
  }

  public void setTolerations(List<Toleration> tolerations) {
    this.tolerations = tolerations;
  }

}
