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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.EdgeWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;

import java.util.List;

/**
 * Combines the results of a join between ExpandIntermediateResults and an edge embedding by growing
 * the intermediate result.
 * Before growing it is checked whether distinctiveness conditions would still apply.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
@FunctionAnnotation.ForwardedFieldsSecond("f2")
public class MergeExpandEmbeddings
  extends RichFlatJoinFunction<ExpandEmbedding, EdgeWithTiePoint, ExpandEmbedding> {

  /**
   * Holds the index of all vertex columns that should be distinct
   */
  private final List<Integer> distinctVertices;
  /**
   * Holds the index of all edge columns that should be distinct
   */
  private final List<Integer> distinctEdges;
  /**
   * Specifies a base column that should be equal to the paths end node
   */
  private final int closingColumn;

  /**
   * Create a new Combine Expand Embeddings Operator
   * @param distinctVertices distinct vertex columns
   * @param distinctEdges distinct edge columns
   * @param closingColumn base column that should be equal to a paths end node
   */
  public MergeExpandEmbeddings(List<Integer> distinctVertices,
    List<Integer> distinctEdges, int closingColumn) {

    this.distinctVertices = distinctVertices;
    this.distinctEdges = distinctEdges;
    this.closingColumn = closingColumn;
  }

  @Override
  public void join(ExpandEmbedding base, EdgeWithTiePoint edge,
    Collector<ExpandEmbedding> out) throws Exception {

    if (checkDistinctiveness(base, edge)) {
      out.collect(base.grow(edge));
    }
  }

  /**
   * Checks the distinctiveness criteria for the expansion
   * @param prev previous intermediate result
   * @param edge edge along which we expand
   * @return true if distinct criteria apply for the expansion
   */
  private boolean checkDistinctiveness(ExpandEmbedding prev, EdgeWithTiePoint edge) {
    if (distinctVertices.isEmpty() && distinctEdges.isEmpty()) {
      return true;
    }

    // the new candidate is invalid under vertex isomorphism
    if (edge.getSource().equals(edge.getTarget()) &&
      !distinctVertices.isEmpty()) {
      return false;
    }

    // check if there are any clashes in the path
    for (GradoopId ref : prev.getPath()) {
      if ((ref.equals(edge.getSource()) || ref.equals(edge.getTarget()) &&
        !distinctVertices.isEmpty()) || (ref.equals(edge.getId()) && !distinctEdges.isEmpty())) {
        return false;
      }
    }

    List<GradoopId> ref;

    // check for clashes with distinct vertices in the base
    for (int i : distinctVertices) {
      ref = prev.getBase().getIdAsList(i);
      if ((ref.contains(edge.getTarget()) && i != closingColumn) ||
        ref.contains(edge.getSource())) {
        return false;
      }
    }

    // check for clashes with distinct edges in the base
    ref = prev.getBase().getIdsAsList(distinctEdges);
    return !ref.contains(edge.getId());
  }
}
