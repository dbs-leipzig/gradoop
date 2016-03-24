package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotation;

public class SalesQuotationSentTo implements
  JoinFunction<SalesQuotation,MasterDataObject,SalesQuotation> {

  @Override
  public SalesQuotation join(SalesQuotation salesQuotation,
    MasterDataObject sentTo) throws Exception {

    if(sentTo == null) {
      System.out.println("\t" + salesQuotation);
    } else {
      salesQuotation.setSentToQuality(sentTo.getQuality());
    }
    return salesQuotation;
  }
}
