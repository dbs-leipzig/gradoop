package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotation;

public class SalesQuotationSentTo implements
  JoinFunction<SalesQuotation,Tuple2<Long, Short>,SalesQuotation> {

  @Override
  public SalesQuotation join(SalesQuotation salesQuotation,
    Tuple2<Long, Short> sentToQuality) throws Exception {

    if(sentToQuality != null) {
      salesQuotation.setSentToQuality(sentToQuality.f1);
    }

    return salesQuotation;
  }
}
