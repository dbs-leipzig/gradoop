package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;

/**
 * (caseId, productId, productQuality)
 */
public class SalesQuotationLine extends Tuple3<GradoopId, Long, Short> {

  public SalesQuotationLine() {
    setContainsQuality((short) 0);
  }

  public void setPartOf(GradoopId salesQuotationId) {
    this.f0 = salesQuotationId;
  }

  public void setContains(Long contains) {
    this.f1 = contains;
  }

  public Short getQuality() {
    return this.f2;
  }

  public void setContainsQuality(Short containsQuality) {
    this.f2 = containsQuality;
  }
}
