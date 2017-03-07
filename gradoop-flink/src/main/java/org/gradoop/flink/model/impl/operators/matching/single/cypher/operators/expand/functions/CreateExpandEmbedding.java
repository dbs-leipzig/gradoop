/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.EdgeWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;

import java.util.List;

/**
 * Creates the initial expand embeddings
 */
@FunctionAnnotation.ReadFieldsSecond("f1; f2")
public class CreateExpandEmbedding
  extends RichFlatJoinFunction<Embedding, EdgeWithTiePoint, ExpandEmbedding> {

  /**
   * Holds the index of all base vertex columns that should be distinct
   */
  private final List<Integer> distinctVertices;
  /**
   * Holds the index of all base edge columns that should be distinct
   */
  private final List<Integer> distinctEdges;
  /**
   * Specifies a base column that should be equal to the paths end node
   */
  private final int closingColumn;


  /**
   * Create new FlatJoin Function
   * @param distinctVertices indices of distinct vertex columns
   * @param distinctEdges indices of distinct edge columns
   * @param closingColumn base column that should be equal to a paths end node
   */
  public CreateExpandEmbedding(List<Integer> distinctVertices,
    List<Integer> distinctEdges, int closingColumn) {

    this.distinctVertices = distinctVertices;
    this.distinctEdges = distinctEdges;
    this.closingColumn = closingColumn;
  }

  @Override
  public void join(Embedding input, EdgeWithTiePoint edge, Collector<ExpandEmbedding> out)
      throws Exception {

    if (checkDistinctiveness(input, edge)) {
      GradoopId[] path = new GradoopId[]{edge.getId(), edge.getTarget()};
      out.collect(new ExpandEmbedding(input, path));
    }
  }

  /**
   * Checks the distinct criteria for the expansion
   * @param input the base part of the expansion
   * @param edge edge along which we expand
   * @return true if distinct criteria hold for the expansion
   */
  private boolean checkDistinctiveness(Embedding input, EdgeWithTiePoint edge) {
    GradoopId edgeId = edge.getId();
    GradoopId tgt = edge.getTarget();

    for (int i : distinctVertices) {
      if (input.getIdAsList(i).contains(tgt) && i != closingColumn) {
        return false;
      }
    }

    for (int i : distinctEdges) {
      if (input.getIdAsList(i).contains(edgeId)) {
        return false;
      }
    }

    return true;
  }
}
