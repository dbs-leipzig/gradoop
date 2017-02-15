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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins;

import com.sun.istack.Nullable;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.edgesemantics.PredefinedEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.Triple;

/**
 * Joins two graphs by using predicates as the sole possible condition to
 *
 * Created by Giacomo Bergami on 31/01/17.
 */
public class GraphThetaJoinWithJoins extends GeneralJoinWithJoinsPlan {

  /**
   * Performs the graph join using the default label concatenation functions and using
   * all the predicates as true constants
   * @param vertexJoinType                Defines the general graph join type providing the way
   *                                      the vertices are joined together
   * @param edgeSemanticsImplementation   Defines the most general semantic for combining the edges
   */
  public GraphThetaJoinWithJoins(JoinType vertexJoinType,
    GeneralEdgeSemantics edgeSemanticsImplementation) {
    super(vertexJoinType, edgeSemanticsImplementation, null, null,
      null, null, null,
      null, null, null);
  }

  /**
   * Joining the two graphs using a custom-defined edge semantics
   *
   * @param vertexJoinType                Defines the general graph join type providing the way
   *                                      the vertices are joined together
   * @param edgeSemanticsImplementation   Defines the most general semantic for combining the edges
   * @param leftHash                      Prior to the join phase, the left elements are coGrouped
   *                                      by their hash value. This is applied to the elements
   *                                      belonging to the left operand
   * @param rightHash                     Prior to the join phase, the left elements are coGrouped
   *                                      by their hash value. This is applied to the elements
   *                                      belonging to the right opreand
   * @param thetaVertex                   Binary function determining which vertices have to be
   *                                      merged
   * @param thetaGraph                    Binary function determining which graphs have to be
   *                                      medged
   * @param vertexLabelConcatenation      Function providing a concatenation for the vertex labels
   * @param graphLabelConcatenation       Function providing a concatenation for the graph head
   *                                      labels
   */
  public GraphThetaJoinWithJoins(JoinType vertexJoinType,
    GeneralEdgeSemantics edgeSemanticsImplementation,
    @Nullable Function<Vertex, Long> leftHash, @Nullable Function<Vertex, Long> rightHash,
    @Nullable Function<Vertex, Function<Vertex, Boolean>> thetaVertex,
    @Nullable Function<GraphHead, Function<GraphHead, Boolean>> thetaGraph,
    @Nullable Function<Tuple2<String, String>, String> vertexLabelConcatenation,
    @Nullable Function<Tuple2<String, String>, String> graphLabelConcatenation) {
    super(vertexJoinType, edgeSemanticsImplementation, null, null, leftHash, rightHash, thetaVertex,
      thetaGraph, vertexLabelConcatenation, graphLabelConcatenation);
  }

  /**
   * Joining the two graphs using a default edge semantics (conjunctive, disjunctive)
   *
   * @param vertexJoinType                Defines the general graph join type providing the way
   *                                      the vertices are joined together
   * @param thetaEdge                     Binary function determining which edges have to be
   *                                      merged
   * @param es                            Edge default semantics
   * @param edgeLabelConcatenation        Function providing a concatenation for the edges
   * @param leftHash                      Prior to the join phase, the left elements are coGrouped
   *                                      by their hash value. This is applied to the elements
   *                                      belonging to the left operand
   * @param rightHash                     Prior to the join phase, the left elements are coGrouped
   *                                      by their hash value. This is applied to the elements
   *                                      belonging to the right opreand
   * @param thetaVertex                   Binary function determining which vertices has to be
   *                                      merged
   * @param thetaGraph                    Binary function determining which graphs have to be
   *                                      medged
   * @param vertexLabelConcatenation      Function providing a concatenation for the vertex labels
   * @param graphLabelConcatenation       Function providing a concatenation for the graph head
   *                                      labels
   */
  public GraphThetaJoinWithJoins(JoinType vertexJoinType,
    final Function<Triple, Function<Triple, Boolean>> thetaEdge,
    final PredefinedEdgeSemantics es,
    @Nullable Function<Tuple2<String, String>, String> edgeLabelConcatenation,
    @Nullable Function<Vertex, Long> leftHash, @Nullable Function<Vertex, Long> rightHash,
    @Nullable Function<Vertex, Function<Vertex, Boolean>> thetaVertex,
    @Nullable Function<GraphHead, Function<GraphHead, Boolean>> thetaGraph,
    @Nullable Function<Tuple2<String, String>, String> vertexLabelConcatenation,
    @Nullable Function<Tuple2<String, String>, String> graphLabelConcatenation) {
    super(vertexJoinType,
      GeneralEdgeSemantics.fromEdgePredefinedSemantics(thetaEdge, es, edgeLabelConcatenation), null,
      null, leftHash, rightHash, thetaVertex, thetaGraph, vertexLabelConcatenation,
      graphLabelConcatenation);
  }

}
