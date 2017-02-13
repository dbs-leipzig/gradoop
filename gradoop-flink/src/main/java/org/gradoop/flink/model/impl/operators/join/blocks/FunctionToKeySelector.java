package org.gradoop.flink.model.impl.operators.join.blocks;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 * Defines a KeySelector from a user-given function
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class FunctionToKeySelector implements KeySelector<Vertex, Long>, Serializable {

  private final Function<Vertex, Long> function;

  public FunctionToKeySelector(Function<Vertex, Long> function) {
    this.function = function;
  }

  public FunctionToKeySelector() {
    this.function = null;
  }

  @Override
  public Long getKey(Vertex value) throws Exception {
    return function == null ? 0L : function.apply(value);
  }
}
