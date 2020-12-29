package tech.stackable.spark.operator.abstractcontroller;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionVersion;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract CRD Controller to work with custom CRDs. Applies given CRD and orders events (add, update, delete)
 * in an blocking queue.
 *
 * @param <Crd>         pojo extending CustomResource
 * @param <CrdList>     pojo extending CustomResourceList
 * @param <CrdDoneable> pojo extending CustomResourceDoneable
 */
public abstract class AbstractCrdController<
  Crd extends CustomResource,
  CrdList extends CustomResourceList<Crd>,
  CrdDoneable extends CustomResourceDoneable<Crd>>
  implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCrdController.class);

  private static final Integer WORKING_QUEUE_SIZE = 1024;

  private final BlockingQueue<String> blockingQueue;
  private final String crdPath;
  private final Long resyncCycle;

  private CustomResourceDefinitionContext crdContext;
  private String namespace;

  private KubernetesClient client;
  private SharedInformerFactory informerFactory;

  private SharedIndexInformer<Crd> crdSharedIndexInformer;
  private Lister<Crd> crdLister;

  private MixedOperation<Crd, CrdList, CrdDoneable, Resource<Crd, CrdDoneable>> crdClient;

  protected AbstractCrdController(KubernetesClient client, String crdPath, Long resyncCycle) {
    this.client = client;
    this.crdPath = crdPath;
    this.resyncCycle = resyncCycle;

    blockingQueue = new ArrayBlockingQueue<>(WORKING_QUEUE_SIZE);

    if (this.client == null) {
      this.client = new DefaultKubernetesClient();
    }

    namespace = this.client.getNamespace();

    if (namespace == null) {
      namespace = "default";
      LOGGER.trace("No namespace found via config, assuming {}", namespace);
    }
  }

  /**
   * load crd context, init informers and crdClient, write crd and register event handlers
   */
  @SuppressWarnings("unchecked")
  public void init() {
    List<HasMetadata> crdMetadata = loadYaml(crdPath);

    informerFactory = client.informers();

    crdContext = getCrdContext(crdMetadata);

    crdSharedIndexInformer = informerFactory.sharedIndexInformerForCustomResource(
      crdContext,
      (Class<Crd>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0],
      (Class<CrdList>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1],
      resyncCycle
    );

    crdLister = new Lister<>(crdSharedIndexInformer.getIndexer(), namespace);

    crdClient = client.customResources(
      crdContext,
      (Class<Crd>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0],
      (Class<CrdList>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1],
      (Class<CrdDoneable>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[2]
    );

    // initialize crd -> should be one for each controller
    writeCrd(namespace, crdMetadata);

    // register events handler
    registerCrdEventHandler();
  }

  /**
   * Build crd context for the sharedIndexInformer or other operations from the crd yaml spec
   *
   * @return crd context
   */
  protected static CustomResourceDefinitionContext getCrdContext(List<HasMetadata> metadata) {

    CustomResourceDefinitionContext.Builder builder = new CustomResourceDefinitionContext.Builder();
    // check metadata
    if (metadata.isEmpty()) {
      LOGGER.error("No crd metadata available!");
      return null;
    }
    if (metadata.size() > 1) {
      LOGGER.warn("multiple crds available ... using first one");
    }

    CustomResourceDefinition crd = (CustomResourceDefinition) metadata.get(0);

    // check spec
    if (crd.getSpec().getVersions().isEmpty()) {
      LOGGER.error("No versions available - return null");
      return null;
    }
    // get served version
    for(CustomResourceDefinitionVersion version : crd.getSpec().getVersions()) {
      // served?
      if(version.getServed()) {
        builder.withVersion(crd.getSpec().getVersions().get(0).getName());
        builder.withKind(crd.getSpec().getNames().getKind());
        builder.withGroup(crd.getSpec().getGroup());
        builder.withScope(crd.getSpec().getScope());
        builder.withPlural(crd.getSpec().getNames().getPlural());

        return builder.build();
      }
    }
    LOGGER.error("No served version found in crd: {}", crd);
    return null;
  }

  /**
   * Overwrite method to specify what should be done after the blocking queue has elements
   *
   * @param crd specified crd resource class for that controller
   */
  protected abstract void process(Crd crd);

  /**
   * Overwrite method to add more informers to be synced (e.g. pods)
   */
  protected void waitForAllInformersSynced() {
    while (!crdSharedIndexInformer.hasSynced()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted during informer sync: {}", e.getMessage());
      }
    }
  }

  /**
   * Load yaml crd from file path
   *
   * @param path path to yaml file
   *
   * @return List<HasMetadata> of that crd
   */
  public List<HasMetadata> loadYaml(String path) {
      try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
        List<HasMetadata> result = client.load(is).get();
        return client.resourceList(result).inNamespace(namespace).get();
      }
      catch (IOException e) {
        LOGGER.error("Error loading YAML from {}", path, e);
        return null;
      }
    }

  /**
   * Create or replace the crd in the API Server
   *
   * @param namespace given namespace
   * @param metaData  List<HasMetadata> from the yaml file
   *
   */
  private void writeCrd(String namespace, List<HasMetadata> metaData) {
    client.resourceList(metaData).inNamespace(namespace).createOrReplace();
  }

  /**
   * Waits until all informers are synced and waits for elements to be in the queue and runs process()
   */
  @Override
  public void run() {
    // init controller
    init();
    // add informers
    informerFactory.addSharedInformerEventListener(exception -> LOGGER.error("Tip: missing/bad crds? {}", exception.getMessage()));
    // start informers
    informerFactory.startAllRegisteredInformers();
    // wait until informers have synchronized
    waitForAllInformersSynced();
    // loop for synchronization
    while (true) {
      try {
        String key = blockingQueue.take();
        Objects.requireNonNull(key, "Key can't be null");

        if (key.isEmpty() || !key.contains("/")) {
          continue;
        }
        // Get the crds resource's name from key which is in format namespace/name
        Crd crd = crdLister.get(key.split("/")[1]);

        if (crd != null) {
          process(crd);
        }
      } catch (InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void onCrdAdd(Crd crd) {
    enqueueCrd(crd);
  }

  private void onCrdUpdate(Crd crdOld, Crd crdNew) {
    enqueueCrd(crdNew);
  }

  private void onCrdDelete(Crd crd, boolean deletedFinalStateUnknown) {}

  /**
   * Register crd event handlers for add, update and delete
   */
  private void registerCrdEventHandler() {
    crdSharedIndexInformer.addEventHandler(new ResourceEventHandler<>() {
      @Override
      public void onAdd(Crd crd) {
        LOGGER.trace("onAdd: {}", crd);
        onCrdAdd(crd);
      }

      @Override
      public void onUpdate(Crd crdOld, Crd crdNew) {
        LOGGER.trace("onUpdate:\ncrdOld: {}\ncrdNew: {}", crdOld, crdNew);
        onCrdUpdate(crdOld, crdNew);
      }

      @Override
      public void onDelete(Crd crd, boolean deletedFinalStateUnknown) {
        LOGGER.trace("onDelete: {}", crd);
        onCrdDelete(crd, deletedFinalStateUnknown);
      }
    });
  }

  /**
   * Add elements to the blocking queue
   *
   * @param crd your controller crd class
   */
  protected void enqueueCrd(Crd crd) {
    String key = Cache.metaNamespaceKeyFunc(crd);
    if (key != null && !key.isEmpty()) {
      blockingQueue.add(key);
    }
  }

  /**
   * Return mixed operation for this crd controller
   *
   * @return MixedOperation for crd of this controller
   */
  public MixedOperation<Crd, CrdList, CrdDoneable, Resource<Crd, CrdDoneable>> getCrdClient() {
    return crdClient;
  }

  protected CustomResourceDefinitionContext getCrdContext() {
    return crdContext;
  }

  protected String getNamespace() {
    return namespace;
  }

  protected Long getResyncCycle() {
    return resyncCycle;
  }

  public KubernetesClient getClient() {
    return client;
  }

  protected SharedInformerFactory getInformerFactory() {
    return informerFactory;
  }

  public SharedIndexInformer<Crd> getCrdSharedIndexInformer() {
    return crdSharedIndexInformer;
  }

  protected Lister<Crd> getCrdLister() {
    return crdLister;
  }

}
