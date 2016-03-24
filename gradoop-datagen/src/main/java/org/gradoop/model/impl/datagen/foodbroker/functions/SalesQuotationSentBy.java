package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotation;

public class SalesQuotationSentBy implements
  JoinFunction<SalesQuotation,MasterDataObject,SalesQuotation> {

  @Override
  public SalesQuotation join(SalesQuotation salesQuotation,
    MasterDataObject sentByQuality) throws Exception {

    salesQuotation.setSentByQuality(sentByQuality.f1);

    return salesQuotation;
  }
}
