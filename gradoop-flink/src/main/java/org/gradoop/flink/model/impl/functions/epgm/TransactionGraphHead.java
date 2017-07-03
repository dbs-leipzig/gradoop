
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Set;

/**
 * (graphHead, {vertex,..}, {edge,..}) => graphHead
 */
@FunctionAnnotation.ForwardedFields("f0->*")
@FunctionAnnotation.ReadFields("f0")
public class TransactionGraphHead implements
  MapFunction<Tuple3<GraphHead, Set<Vertex>, Set<Edge>>, GraphHead> {

  @Override
  public GraphHead map(Tuple3<GraphHead, Set<Vertex>, Set<Edge>> triple) throws Exception {
    return triple.f0;
  }
}
