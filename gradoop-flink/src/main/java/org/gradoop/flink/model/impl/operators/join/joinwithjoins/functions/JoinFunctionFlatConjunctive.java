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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.Triple;

/**
 * Implements the conjunctive semantic by defining a way to determine the edges by the resulting
 * triples.
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class JoinFunctionFlatConjunctive implements
  FlatJoinFunction<Triple, Triple, Edge> {

  /**
   * Condition for the triples, checking if the condition holds. The source and destination vertices
   * are the one in the resulting graph, so they provide the values coming from at least one graph
   * operand
   */
  private final Function<Tuple2<Triple, Triple>, Boolean> finalThetaEdge;

  /**
   * Utility function for combining the edges into one final result
   */
  private final OplusEdges combineEdges;

  /**
   * Default constructor
   * @param finalThetaEdge    Function used to select the patterns
   * @param combineEdges      Function used to merge the matching edges
   */
  public JoinFunctionFlatConjunctive(
    Function<Tuple2<Triple, Triple>, Boolean> finalThetaEdge,
    OplusEdges combineEdges) {
    this.finalThetaEdge = finalThetaEdge;
    this.combineEdges = combineEdges;
  }

  @Override
  public void join(Triple first, Triple second,
    Collector<Edge> out) throws Exception {
    // Iff. the triples match with both source and destination,
    // then I have to merge the edges into one single edge
    if (first.f0.getId().equals(second.f0.getId()) && first.f2.getId().equals(second.f2.getId())) {
      if (finalThetaEdge.apply(new Tuple2<>(first, second))) {
        Edge prepared = combineEdges.apply(new Tuple2<>(first.f1, second.f1));
        prepared.setSourceId(first.f0.getId());
        prepared.setTargetId(first.f2.getId());
        out.collect(prepared);
      }
    }
  }
}
