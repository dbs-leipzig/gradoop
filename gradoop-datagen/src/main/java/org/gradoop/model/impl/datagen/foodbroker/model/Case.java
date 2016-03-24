package org.gradoop.model.impl.datagen.foodbroker.model;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

public class Case<V extends EPGMVertex, E extends EPGMEdge> {

//  private V salesQuotation;
  private MasterDataReference salesQuotationSentBy;

  public Case() {

  }

//  public V getSalesQuotation() {
//    return salesQuotation;
//  }
//
//  public void setSalesQuotation(V salesQuotation) {
//    this.salesQuotation = salesQuotation;
//  }

  public MasterDataReference getSalesQuotationSentBy() {
    return salesQuotationSentBy;
  }

  public void setSalesQuotationSentBy(
    MasterDataReference salesQuotationSentBy) {
    this.salesQuotationSentBy = salesQuotationSentBy;
  }




  public String toString() {
    return
// salesQuotation.getLabel() + " : " + salesQuotation.getId() +
      (salesQuotationSentBy != null ?
      "\n\tsentBy : " + salesQuotationSentBy.getTargetId().toString() : "");
  }


}
