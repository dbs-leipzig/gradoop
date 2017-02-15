package org.gradoop.flink.model.impl.operators.join.joinwithfusion;

import org.apache.flink.api.java.functions.FunctionAnnotation;

/**
 * Created by vasistas on 15/02/17.
 */
@FunctionAnnotation.ForwardedFields("*->*")
public class Self<K> implements
  org.apache.flink.api.java.functions.KeySelector<K, K> {
  @Override
  public K getKey(K value) throws Exception {
    return value;
  }
}
