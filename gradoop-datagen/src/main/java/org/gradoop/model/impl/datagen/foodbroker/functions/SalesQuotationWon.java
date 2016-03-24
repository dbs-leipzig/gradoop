package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotation;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Created by peet on 24.03.16.
 */
public class SalesQuotationWon implements FilterFunction<SalesQuotation> {

  @Override
  public boolean filter(SalesQuotation salesQuotation) throws Exception {
    return salesQuotation.getProperties().get(SalesQuotation.STATUS_KEY)
      .equals(PropertyValue.create(SalesQuotation.STATUS_WON));
  }
}
