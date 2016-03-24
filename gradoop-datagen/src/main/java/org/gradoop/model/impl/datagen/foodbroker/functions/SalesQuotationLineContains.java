package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;

public class SalesQuotationLineContains implements
  JoinFunction<TransactionalDataObject,MasterDataObject, TransactionalDataObject> {

  @Override
  public TransactionalDataObject join(
    TransactionalDataObject line, MasterDataObject product) throws Exception {

    line.getReferences()
      .put(SalesQuotationLine.CONTAINS, product.getId());
    line.getQualities()
      .put(SalesQuotationLine.CONTAINS, product.getQuality());

    return line;
  }
}
