
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.common.model.impl.pojo.GraphHead;

import java.util.Set;

/**
 * graphTransaction <=> (graphHead, {vertex,..}, {edge, ..})
 *
 * Forwarded fields:
 *
 * f0: transaction graph head
 * f1: transaction vertices
 * f2: transaction edges
 */
@FunctionAnnotation.ForwardedFields("f0;f1;f2")
public class GraphTransactionTriple
  implements MapFunction<GraphTransaction, Tuple3<GraphHead, Set<Vertex>, Set<Edge>>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple3<GraphHead, Set<Vertex>, Set<Edge>> reuseTuple = new Tuple3<>();

  @Override
  public Tuple3<GraphHead, Set<Vertex>, Set<Edge>> map(GraphTransaction transaction)
    throws Exception {

    reuseTuple.f0 = transaction.getGraphHead();
    reuseTuple.f1 = transaction.getVertices();
    reuseTuple.f2 = transaction.getEdges();

    return reuseTuple;
  }
}
