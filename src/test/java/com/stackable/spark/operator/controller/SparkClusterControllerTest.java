package com.stackable.spark.operator.controller;

public class SparkClusterControllerTest {
//	@Rule
//	public KubernetesServer server = new KubernetesServer();
//	
//	public KubernetesClient client;
//	public SharedInformerFactory informerFactory;
//	
//	public SparkClusterController controller;
//	
//	public String namespace = "default";
//	public String crdPath = "spark-cluster-crd.yaml";
//	public long resyncCycle = 5 * 1000L;
//	
//    @Before
//    public void setUp() {
//    	client = server.getClient();
//    	informerFactory = client.informers();
//    	controller = new SparkClusterController(client, informerFactory, namespace, crdPath, resyncCycle);
//    	informerFactory.startAllRegisteredInformers();
//    }
//
//    @After
//    public void tearDown() {
//    	informerFactory.stopAllRegisteredInformers();
//    	client.close();
//    }
//	
//	@Test
//	@DisplayName("Should be able to list pods in default namespace")
//	public void testCreateSparkClusterController() {
//	    // Given
//	    server.expect().get().withPath("/api/v1/namespaces/default/pods")
//	            .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder().build())
//	            .once();
//	    
//	    KubernetesClient client = server.getClient();
//
//	    // When
//	    PodList podList = client.pods().inNamespace("default").list();
//
//	    // Then
//	    assertTrue(podList.getItems().isEmpty());
//	}
}
