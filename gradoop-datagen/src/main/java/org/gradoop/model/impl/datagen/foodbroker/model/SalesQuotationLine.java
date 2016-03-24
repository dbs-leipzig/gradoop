package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;

/**
 * (caseId, productId, productQuality)
 */
public class SalesQuotationLine extends Tuple3<GradoopId, Long, Short> {

  public SalesQuotationLine() {

  }

  public void setPartOf(GradoopId caseId) {
    this.f0 = caseId;
  }

  public void setCountains(Long contains) {
    this.f1 = contains;
    this.f2 = (short) 0;
  }

  public Short getQuality() {
    return this.f2;
  }

  public void setContainsQuality(Short containsQuality) {
    this.f2 = containsQuality;
  }
}
