package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;
import org.gradoop.model.impl.properties.PropertyValue;

public class SalesQuotationWon
  implements FilterFunction<TransactionalDataObject> {

  @Override
  public boolean filter(TransactionalDataObject salesQuotation) throws Exception {
    return salesQuotation.getProperties().get(SalesQuotation.STATUS_KEY)
      .equals(PropertyValue.create(SalesQuotation.STATUS_WON));
  }
}
