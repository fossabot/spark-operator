package tech.stackable.spark.operator.application;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.stackable.spark.operator.abstractcontroller.AbstractCrdController;
import tech.stackable.spark.operator.application.crd.SparkApplication;
import tech.stackable.spark.operator.application.crd.SparkApplicationSpec;
import tech.stackable.spark.operator.cluster.crd.SparkNodeMaster;
import tech.stackable.spark.operator.common.fabric8.SparkApplicationDoneable;
import tech.stackable.spark.operator.common.fabric8.SparkApplicationList;
import tech.stackable.spark.operator.common.type.SparkConfig;

public class SparkApplicationController extends AbstractCrdController<SparkApplication, SparkApplicationList, SparkApplicationDoneable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkApplicationController.class);

  private static final int TIMEOUT = 120;
  private final Set<SparkApplication> workingQueue = new HashSet<>();

  public SparkApplicationController(KubernetesClient client, String crdPath, Long resyncCycle) {
    super(client, crdPath, resyncCycle);
  }

  @Override
  public void process(SparkApplication crd) {
    LOGGER.trace("Got CRD: {}", crd.getMetadata().getName());
    launch(crd);
  }

  /**
   * Launch the given application, transform config parameters, set executor and driver options etc.
   *
   * @param app spark application configured via yaml/json
   *
   */
  private void launch(SparkApplication app) {
    // do not act on processed apps
    if (workingQueue.contains(app)) {
      return;
    }
    workingQueue.add(app);

    SparkApplicationSpec spec = app.getSpec();
    try {
      CountDownLatch countDownLatch = new CountDownLatch(1);
      SparkApplicationListener sparkAppListener = new SparkApplicationListener(countDownLatch);

      URL sparkHome = ClassLoader.getSystemResource("spark-3.0.1-bin-hadoop2.7");
      LOGGER.debug("Start app: {} - SPARK_HOME: {}", app.getMetadata().getName(), sparkHome.getPath());

      SparkLauncher launcher = new SparkLauncher();
      launcher.setSparkHome(sparkHome.getPath());
      launcher.setAppResource(spec.getMainApplicationFile());
      launcher.setMainClass(spec.getMainClass());
      // TODO get host
      launcher.setMaster(getMasterNodeName());
      launcher.setDeployMode(spec.getMode());
      launcher.setAppName(app.getMetadata().getName());
      // conf from app
      if (spec.getDriver().getCores() != null) {
        launcher.setConf(SparkConfig.SPARK_DRIVER_CORES.getConfig(), spec.getDriver().getCores());
      }
      if (spec.getDriver().getMemory() != null) {
        launcher.setConf(SparkConfig.SPARK_DRIVER_MEMORY.getConfig(), spec.getDriver().getMemory());
      }
      if (spec.getExecutor().getCores() != null) {
        launcher.setConf(SparkConfig.SPARK_EXECUTOR_CORES.getConfig(), spec.getExecutor().getCores());
      }
      if (spec.getExecutor().getMemory() != null) {
        launcher.setConf(SparkConfig.SPARK_EXECUTOR_MEMORY.getConfig(), spec.getExecutor().getMemory());
      }
      if (spec.getSecret() != null) {
        launcher.setConf(SparkConfig.SPARK_AUTHENTICATE.getConfig(), "true");
      }
      if (spec.getSecret() != null) {
        launcher.setConf(SparkConfig.SPARK_AUTHENTICATE_SECRET.getConfig(), spec.getSecret());
      }
      // add other spark configuration
      for (EnvVar envVar : spec.getSparkConfiguration()) {
        String name = envVar.getName();
        String value = envVar.getValue();
        if (name != null && !name.isEmpty() && value != null && !value.isEmpty()) {
          launcher.setConf(name, value);
        }
      }

      if (spec.getArgs().size() > 0) {
        launcher.addAppArgs(spec.getArgs().toArray(String[]::new));
      }
      // start with listener
      launcher.startApplication(sparkAppListener);

      Thread sparkAppListenerThread = new Thread(sparkAppListener);
      sparkAppListenerThread.start();

      countDownLatch.await(TIMEOUT, TimeUnit.SECONDS);

      // delete app when finished
      // TODO: check for sleep / timer if repeating
      workingQueue.remove(app);
      getCrdClient().delete(app);
    } catch (InterruptedException | IOException e) {
      LOGGER.error(e.getMessage());
    }
  }

  /**
   * Return master node name and port for given cluster
   *
   * @return spark master host name
   */
  private String getMasterNodeName() {
    PodList podList = getClient().pods().inNamespace(getNamespace()).list();

    if (podList == null) {
      LOGGER.warn("no pods found in namespace {}", getNamespace());
      return null;
    }

    List<Pod> pods = podList.getItems();
    for (Pod pod : pods) {
      if (pod.getMetadata().getOwnerReferences().isEmpty() ||
        !pod.getMetadata().getOwnerReferences().get(0).getKind().equals("SparkCluster")) {
        continue;
      }

      // has node name?
      String nodeName = pod.getSpec().getNodeName();
      if (nodeName == null) {
        continue;
      }

      // is master?
      if (!pod.getMetadata().getName().contains(SparkNodeMaster.POD_TYPE)) {
        continue;
      }

      // has container?
      if (pod.getSpec().getContainers().isEmpty()) {
        continue;
      }

      String port = "7077";

      // has SPARK_MASTER_PORT?
      for (Container container : pod.getSpec().getContainers()) {
        if (container.getEnv().isEmpty()) {
          continue;
        }

        for (EnvVar envVar : container.getEnv()) {
          if (envVar.getName().equals(SparkConfig.SPARK_MASTER_PORT.toEnv())) {
            port = envVar.getValue();
          }
        }
      }

      //construct
      return "spark://" + nodeName + ":" + port;
    }

    return null;
  }

  private static final class SparkApplicationListener implements SparkAppHandle.Listener, Runnable {
    private final CountDownLatch countDownLatch;

    private SparkApplicationListener(CountDownLatch countDownLatch) {
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void stateChanged(SparkAppHandle handle) {
      String sparkAppId = handle.getAppId();
      State appState = handle.getState();
      if (sparkAppId != null) {
        LOGGER.info("spark job[{}] state changed to: {}", sparkAppId, appState);
      } else {
        LOGGER.info("spark job state changed to: {}", appState);
      }
      if (appState != null && appState.isFinal()) {
        LOGGER.info("spark job[{}] state changed to: {}", sparkAppId, appState);
        countDownLatch.countDown();
      }
    }

    @Override
    public void infoChanged(SparkAppHandle handle) {}

    @Override
    public void run() {}
  }

}
