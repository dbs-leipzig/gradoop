package org.gradoop.flink.model.impl.operators.nest.logic;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.nest.functions.AssociateAndMark;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectVertices;
import org.gradoop.flink.model.impl.operators.nest.functions.Hex4;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestedResult;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Created by vasistas on 06/04/17.
 */
public class NestedModelOperations {

  public static NestedResult nesting(NestingIndex graphIndex, NestingIndex collectionIndex) {
    // Associate each gid in hypervertices.H to the merged vertices
    DataSet<GradoopId> heads = graphIndex.getGraphHeads();

    // Mark each vertex if either it's present or not in the final match
    // TODO       JOIN COUNT: (1)
    DataSet<Hexaplet> nestedResult = graphIndex.getGraphHeadToVertex()
      .leftOuterJoin(collectionIndex.getGraphHeadToVertex())
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      // If the vertex does not appear in the graph collection, the f2 element is null.
      // These vertices are the ones to be returned as vertices alongside with the new
      // graph heads
      .with(new AssociateAndMark());

    // Vertices to be returend within the NestedIndexing
    DataSet<GradoopId> tmpVert = nestedResult
      .groupBy(new Hex4())
      .reduceGroup(new CollectVertices());

    DataSet<Tuple2<GradoopId, GradoopId>> vertices = heads
      .crossWithHuge(tmpVert);

    DataSet<GradoopId> tmpEdges = graphIndex
      .getGraphHeadToEdge()
      .map(new Value1Of2<>());

    DataSet<Tuple2<GradoopId, GradoopId>> edges = heads
      .crossWithHuge(tmpEdges);

    return new NestedResult(heads, vertices, edges, nestedResult);
  }


}
