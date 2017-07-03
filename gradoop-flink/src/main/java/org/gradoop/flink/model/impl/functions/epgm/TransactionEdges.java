
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Set;

/**
 * (graphHead, {vertex,..}, {edge,..}) => edge,..
 */
public class TransactionEdges implements
  FlatMapFunction<Tuple3<GraphHead, Set<Vertex>, Set<Edge>>, Edge> {

  @Override
  public void flatMap(Tuple3<GraphHead, Set<Vertex>, Set<Edge>> graphTriple,
    Collector<Edge> collector) throws Exception {

    graphTriple.f2.forEach(collector::collect);
  }
}
