package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;


public class ForeignKey implements KeySelector<TransactionalDataObject, Long> {

  @Override
  public Long getKey(TransactionalDataObject transactionalDataObject) throws
    Exception {
    return transactionalDataObject.getForeignKey();
  }
}
