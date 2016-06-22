package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.properties.PropertyValue;

import java.util.ArrayList;
import java.util.List;

/**
 * GroupReduceFunction to Reduce duplicated original id's from the vertex set
 *
 * @param <V> EPGM vertex type class
 */
public class VertexReducer<V extends EPGMVertex> implements
  GroupReduceFunction<V, V> {
  /**
   * Reduces duplicated original id's from vertex set
   *
   * @param iterable    vertex set
   * @param collector   reduced collection
   * @throws Exception
   */
  @Override
  public void reduce(Iterable<V> iterable, Collector<V> collector) throws
    Exception {
    List<PropertyValue> ids = new ArrayList<>();
    for (V vex : iterable) {
      if (!ids.contains(vex.getPropertyValue("id"))) {
        collector.collect(vex);
      }
      ids.add(vex.getPropertyValue("id"));
    }
  }
}
