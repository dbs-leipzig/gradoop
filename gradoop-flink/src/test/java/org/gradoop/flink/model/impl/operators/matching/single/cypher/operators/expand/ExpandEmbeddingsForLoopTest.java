package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

public class ExpandEmbeddingsForLoopTest extends ExpandEmbeddingsTest {

  protected ExpandEmbeddings getOperator(
    DataSet<Embedding> input, DataSet<Embedding> candidateEdges,
    int expandColumn, int lowerBound, int upperBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn) {

    return new ExpandEmbeddingsForLoop(input, candidateEdges, expandColumn, lowerBound,
      upperBound, direction, distinctVertexColumns, distinctEdgeColumns, closingColumn);
  }

}
