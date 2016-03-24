package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;

public class SalesQuotationSentTo implements
  JoinFunction<TransactionalDataObject, MasterDataObject, TransactionalDataObject> {


  @Override
  public TransactionalDataObject join(TransactionalDataObject quotation,
    MasterDataObject customer) throws Exception {

    quotation.getReferences().put(SalesQuotation.SENT_TO, customer.getId());
    quotation.getQualities().put(SalesQuotation.SENT_TO, customer.getQuality());

    return quotation;
  }
}
