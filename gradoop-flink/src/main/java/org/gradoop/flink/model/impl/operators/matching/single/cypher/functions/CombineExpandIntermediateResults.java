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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandIntermediateResult;

import java.util.List;

/**
 * Expands a given Embedding by an Edge
 */
public class CombineExpandIntermediateResults
  extends RichFlatJoinFunction<ExpandIntermediateResult, Embedding, ExpandIntermediateResult> {

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
  private final int circle;

  /**
   * Create a new Combine Expand Embeddings Operator
   * @param distinctVertices distinct vertex columns
   * @param distinctEdges distinct edge columns
   * @param circle base column that should be equal to a paths end node
   */
  public CombineExpandIntermediateResults(List<Integer> distinctVertices,
    List<Integer> distinctEdges, int circle) {

    this.distinctVertices = distinctVertices;
    this.distinctEdges = distinctEdges;
    this.circle = circle;
  }

  @Override
  public void join(ExpandIntermediateResult base, Embedding extension,
    Collector<ExpandIntermediateResult> out) throws Exception {

    if (checkDistinctiveness(base, extension)) {
      out.collect(base.grow(extension));
    }
  }

  /**
   * Checks the distinct criteria for the expansion
   * @param prev previous intermediate result
   * @param extension edge along which we expand
   * @return true if distinct criteria hold for the expansion
   */
  private boolean checkDistinctiveness(ExpandIntermediateResult prev, Embedding extension) {
    if (distinctVertices.isEmpty() && distinctEdges.isEmpty()) {
      return true;
    }

    GradoopId src = extension.getEntry(0).getId();
    GradoopId edge = extension.getEntry(1).getId();
    GradoopId tgt = extension.getEntry(2).getId();

    // the new edge does hold for vertex isomorphism
    if (src.equals(tgt) && !distinctVertices.isEmpty()) {
      return false;
    }

    // check if there are any clashes in the path
    for (GradoopId ref : prev.getPath()) {
      if ((ref.equals(src) || ref.equals(tgt) && !distinctVertices.isEmpty()) ||
          (ref.equals(edge) && !distinctEdges.isEmpty())) {

        return false;
      }
    }

    // check for clashes with distinct vertices in the base
    for (int i : distinctVertices) {
      GradoopId ref = prev.getBase().getEntry(i).getId();
      if ((ref.equals(tgt) && i != circle) || ref.equals(src)) {
        return false;
      }
    }

    // check for clashes with distinct edges in the base
    for (int i : distinctEdges) {
      GradoopId ref = prev.getBase().getEntry(i).getId();
      if (ref.equals(edge)) {
        return false;
      }
    }

    return true;
  }
}
