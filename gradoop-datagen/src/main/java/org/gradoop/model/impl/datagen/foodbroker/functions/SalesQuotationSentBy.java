package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotation;

public class SalesQuotationSentBy<V extends EPGMVertex> implements
  JoinFunction<SalesQuotation,MasterDataObject<V>,SalesQuotation> {

  @Override
  public SalesQuotation join(SalesQuotation salesQuotation,
    MasterDataObject<V> sentByQuality) throws Exception {

    if(sentByQuality != null) {
      salesQuotation.setSentByQuality(sentByQuality.f1);
    }

    return salesQuotation;
  }
}
