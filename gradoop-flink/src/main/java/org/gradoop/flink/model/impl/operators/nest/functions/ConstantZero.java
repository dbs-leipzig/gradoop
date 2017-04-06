package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * Created by vasistas on 06/04/17.
 */
public class ConstantZero<K> implements KeySelector<K, Integer> {
  @Override
  public Integer getKey(K k) throws Exception {
    return 0;
  }
}
