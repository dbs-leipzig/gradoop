package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.UnaryFunction;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

import java.util.List;

/**
 * Maps the vertices to Tuple2's, where each tuple contains the vertex
 * and one split values the split values are determined using a user defined
 * function
 *
 * @param <V> EPGM vertex type
 */
public class ExtractSplitValuesFlatMapper<V extends EPGMVertex> implements
  FlatMapFunction<V, Tuple2<GradoopId, PropertyValue>> {
  /**
   * Self defined Function
   */
  private UnaryFunction<V, List<PropertyValue>> function;

  /**
   * Constructor
   *
   * @param function actual defined Function
   */
  public ExtractSplitValuesFlatMapper(
    UnaryFunction<V, List<PropertyValue>> function) {
    this.function = function;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(V vd,
    Collector<Tuple2<GradoopId, PropertyValue>> collector) throws Exception {
    List<PropertyValue> splitKeys = function.execute(vd);
    for (PropertyValue key : splitKeys) {
      collector.collect(new Tuple2<>(vd.getId(), key));
    }
  }
}

