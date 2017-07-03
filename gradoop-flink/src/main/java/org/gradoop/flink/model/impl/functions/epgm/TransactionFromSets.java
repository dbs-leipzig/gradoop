
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.HashSet;
import java.util.Set;

/**
 * (graphHead) =|><| (graphId,{vertex,..},{edge,..}) => (graphHead,{vertex,..},{edge,..})
 *
 * Forwarded fields first:
 *
 * f0: graph head
 *
 * Forwarded fields second:
 *
 * f1: vertex set
 * f2: edge set
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1;f2")
public class TransactionFromSets
  implements JoinFunction<GraphHead, Tuple3<GradoopId, Set<Vertex>, Set<Edge>>, GraphTransaction> {
  /**
   * Used if right hand side is empty
   */
  private static final Set<Vertex> EMPTY_VERTEX_SET = new HashSet<>(0);
  /**
   * Used if right hand side is empty
   */
  private static final Set<Edge> EMPTY_EDGE_SET = new HashSet<>(0);
  /**
   * Reduce object instantiations
   */
  private final GraphTransaction reuseTransaction = new GraphTransaction();

  @Override
  public GraphTransaction join(GraphHead graphHead, Tuple3<GradoopId, Set<Vertex>, Set<Edge>> sets)
    throws Exception {

    reuseTransaction.setGraphHead(graphHead);
    reuseTransaction.setVertices(sets == null ? EMPTY_VERTEX_SET : sets.f1);
    reuseTransaction.setEdges(sets == null ? EMPTY_EDGE_SET : sets.f2);

    return reuseTransaction;
  }
}
