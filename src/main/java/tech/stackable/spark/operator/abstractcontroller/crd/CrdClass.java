package tech.stackable.spark.operator.abstractcontroller.crd;

import io.fabric8.kubernetes.client.CustomResource;

/**
 * CRD class for CRD controller with spec and status
 *
 * @param <Spec>   - crd spec
 * @param <Status> - crd status
 */
public class CrdClass<Spec, Status> extends CustomResource {

  private static final long serialVersionUID = 2364656081820201645L;

  private Spec spec;
  private Status status;

  public CrdClass() {
  }

  public CrdClass(Spec spec, Status status) {
    super();
    this.spec = spec;
    this.status = status;
  }

  public Spec getSpec() {
    return spec;
  }

  public void setSpec(Spec spec) {
    this.spec = spec;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

}
