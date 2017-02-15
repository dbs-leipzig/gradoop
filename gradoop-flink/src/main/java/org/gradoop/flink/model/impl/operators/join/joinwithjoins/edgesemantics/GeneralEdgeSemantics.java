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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.edgesemantics;

import com.sun.istack.Nullable;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.JoinWithJoinsUtils;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.JoinFunctionFlatConjunctive;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.JoinFunctionFlatDisjunctive;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.OplusEdges;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.Triple;

import java.io.Serializable;

/**
 * Defines the generic edge semantics. These could be defined in two possible
 * ways:
 * 1) Automatically inferred by a semantics of choice (disjunctive, conjunctive)
 * 2) Define a way to combine the edges through two possibile basic parameters:
 *    - a way to combine the edges (<code>edgeJoinType</code>) through join operation
 *    - a way to combine the resulting triples into a final edge (<code>joiner</code>)
 *
 * Created by Giacomo Bergami on 30/01/17.
 */
public class GeneralEdgeSemantics implements Serializable {
  /**
   * Defines how to combine the edges through joins
   */
  private final JoinType edgeJoinType;

  /**
   *  Combines the resulting triples into a final edge
   */
  private final FlatJoinFunction<Triple, Triple, Edge> joiner;

  /**
   * General constructor accepting the most generic parameters to perform a join between the edges
   * @param edgeJoinType    how to combine the edges through joins
   * @param joiner          Combines the resulting triples into a final edge
   */
  public GeneralEdgeSemantics(JoinType edgeJoinType,
    FlatJoinFunction<Triple, Triple, Edge> joiner) {
    this.edgeJoinType = edgeJoinType;
    this.joiner = joiner;
  }

  /**
   * Using a PredefinedEdgeSemantics (eg. disjunctive, conjunctive) determines the parameters to
   * instantiate the edge semantics.
   * @param thetaEdge               Function for selecting the triples
   * @param es                      Predefined edge semantics
   * @param edgeLabelConcatenation  How to concatenate the labels from two edges
   * @return                        The instantiation of the edge semantics
   */
  public static GeneralEdgeSemantics fromEdgePredefinedSemantics(
    final Function<Triple, Function<Triple, Boolean>> thetaEdge,
    final PredefinedEdgeSemantics es,
    @Nullable Function<Tuple2<String, String>, String> edgeLabelConcatenation) {
    OplusEdges combinateEdges =
      new OplusEdges(JoinWithJoinsUtils.generateConcatenator(edgeLabelConcatenation));
    return fromEdgePredefinedSemantics(thetaEdge, es, combinateEdges);
  }

  /**
   * Using a PredefinedEdgeSemantics (eg. disjunctive, conjunctive) determines the parameters to
   * instantiate the edge semantics.
   * @param thetaEdge               Function for selecting the triples
   * @param es                      Predefined edge semantics
   * @param combineEdges            How to concatenate two edges
   * @return                        The instantiation of the edge semantics
   */
  private static GeneralEdgeSemantics fromEdgePredefinedSemantics(
    final Function<Triple, Function<Triple, Boolean>> thetaEdge,
    final PredefinedEdgeSemantics es, final OplusEdges combineEdges) {
    JoinType edgeJoinType = null;
    FlatJoinFunction<Triple, Triple, Edge> joiner = null;
    final Function<Tuple2<Triple, Triple>, Boolean> finalThetaEdge =
      JoinWithJoinsUtils.extendBasic2(thetaEdge);
    switch (es) {
    case CONJUNCTIVE:
      edgeJoinType = JoinType.INNER;
      joiner = new JoinFunctionFlatConjunctive(finalThetaEdge, combineEdges);
      break;
    case DISJUNCTIVE:
      edgeJoinType = JoinType.FULL_OUTER;
      joiner = new JoinFunctionFlatDisjunctive(finalThetaEdge, combineEdges);
      break;
    default:
      Object noop;
    }
    return new GeneralEdgeSemantics(edgeJoinType, joiner);
  }

  /**
   *
   * @return  The join type used between the edges
   */
  public JoinType getEdgeJoinType() {
    return edgeJoinType;
  }

  /**
   *
   * @return  The join function used to determine the way to combine triples to edges
   */
  public FlatJoinFunction<Triple, Triple, Edge> getJoiner() {
    return joiner;
  }
}
