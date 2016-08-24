package org.gradoop.flink.model.impl.functions.epgm;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Set;

public class TransactionFromSets
  implements JoinFunction
  <GraphHead, Tuple3<GradoopId, Set<Vertex>, Set<Edge>>, GraphTransaction> {

  @Override
  public GraphTransaction join(GraphHead graphHead,
    Tuple3<GradoopId, Set<Vertex>, Set<Edge>> sets) throws Exception {

    Set<Vertex> vertices;
    Set<Edge> edges;

    if (sets.f0 == null) {
      vertices = Sets.newHashSetWithExpectedSize(0);
      edges = Sets.newHashSetWithExpectedSize(0);
    } else {
      vertices = sets.f1;
      edges = sets.f2;
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }
}
