package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * Filters transactional vertices.
 * @param <V> vertex type.
 */
public class TransactionalData<V extends EPGMVertex>
  implements FilterFunction<V> {

  @Override
  public boolean filter(V v) throws Exception {
    return v.getPropertyValue(BusinessTransactionGraphs.SUPERTYPE_KEY)
      .getString().equals(
        BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL);
  }
}
