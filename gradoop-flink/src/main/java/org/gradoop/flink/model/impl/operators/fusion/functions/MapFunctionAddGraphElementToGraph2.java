
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * A variant of the AddToGraph, where we have no graph head
 * but a graph id
 * @param <K> element receiving the new graph id
 *
 */
public class MapFunctionAddGraphElementToGraph2<K extends GraphElement> implements MapFunction<K, K> {

  /**
   * Graph Id that has to be added
   */
  private final GradoopId newGraphId;

  /**
   * Default constructor
   * @param newGraphId  Graph Id that has to be added
   */
  public MapFunctionAddGraphElementToGraph2(GradoopId newGraphId) {
    this.newGraphId = newGraphId;
  }

  @Override
  public K map(K value) throws Exception {
    value.addGraphId(newGraphId);
    return value;
  }
}
