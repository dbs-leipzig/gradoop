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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.Triple;

/**
 * Implements the disjunctive semantic by defining a way to determine the edges by the resulting
 * triples. Please note that the first and the second triple will be null or not dependingly on the
 * edge join semantic of choice.
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class JoinFunctionFlatDisjunctive extends JoinFunctionFlatConjunctive {

  /**
   * Some private edge to be updated
   */
  private final Edge e;

  /**
   * Default constructor
   * @param finalThetaEdge    Function used to select which patterns are useful in the graphs
   * @param combineEdges      Function to combine edges that have to be merged
   */
  public JoinFunctionFlatDisjunctive(
    Function<Tuple2<Triple, Triple>, Boolean> finalThetaEdge,
    OplusEdges combineEdges) {
    super(finalThetaEdge, combineEdges);
    e = new Edge();
  }

  /**
   * Updates the edges coming directly from the two graph operands by
   * @param triple  Triple containing the to-be-returned edge
   * @return        The new edge to-appear in the final graph join result
   */
  private Edge generateFromSingle(Triple triple) {
    e.setSourceId(triple.f0.getId());
    e.setTargetId(triple.f2.getId());
    e.setProperties(triple.f1.getProperties());
    e.setLabel(triple.f1.getLabel());
    e.setId(GradoopId.get());
    return e;
  }

  @Override
  public void join(Triple first, Triple second,
    Collector<Edge> out) throws Exception {
    // If both edges match with the same source and target vertex in the final graph representation
    if (first != null && second != null) {
      if (first.f0.getId().equals(second.f0.getId()) &&
          first.f2.getId().equals(second.f2.getId())) {
        // If the triples match together, then it means that they have to be merged together
        super.join(first, second, out);
      } else {
        // Otherwise, I keep them separated
        out.collect(generateFromSingle(first));
        out.collect(generateFromSingle(second));
      }
    } else if (first != null) {
      //Otherwise, create the left and right edge for their respective graph
      out.collect(generateFromSingle(first));
    } else {
      out.collect(generateFromSingle(second));
    }
  }
}
