package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;

public class TransactionalData<V extends EPGMVertex> implements
  FilterFunction<V> {
  @Override
  public boolean filter(V v) throws Exception {
    return v.getPropertyValue(BusinessTransactionGraphs.SUPERTYPE_KEY)
      .getString().equals(
        BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL);
  }
}
