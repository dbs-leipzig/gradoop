package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotationLine;

public class SalesQuotationLineContains implements
  JoinFunction<SalesQuotationLine,Tuple2<Long, Short>, SalesQuotationLine> {

  @Override
  public SalesQuotationLine join(SalesQuotationLine salesQuotation,
    Tuple2<Long, Short> productQuality) throws Exception {

    if(productQuality != null) {
      salesQuotation.setContainsQuality(productQuality.f1);
    }

    return salesQuotation;
  }
}
