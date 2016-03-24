package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotationLine;

public class SalesQuotationLineContains implements
  JoinFunction<SalesQuotationLine,MasterDataObject, SalesQuotationLine> {

  @Override
  public SalesQuotationLine join(SalesQuotationLine salesQuotation,
    MasterDataObject product) throws Exception {

    salesQuotation.setContainsQuality(product.getQuality());

    return salesQuotation;
  }
}
