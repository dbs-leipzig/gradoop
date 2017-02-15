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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions.PreFilterRelationalJoin;

/**
 * Instantiates the graph join using edges as a predicate function. (R-Join)
 *
 * Created by Giacomo Bergami on 31/01/17.
 */
public class GraphEdgeJoinWithJoins extends GeneralJoinWithJoinsPlan<GradoopId> {

  /**
   *
   * @param vertexJoinType
   * @param edgeSemanticsImplementation
   * @param relations                     Set of edges used as predicate. Alongside with the
   *                                      vertexJoinType, it induces the definition of the
   *                                      prefiltering function
   */
  public GraphEdgeJoinWithJoins(JoinType vertexJoinType, GeneralEdgeSemantics edgeSemanticsImplementation,
    DataSet<Edge> relations) {
    this(vertexJoinType, edgeSemanticsImplementation, relations, null, null, null, null);
  }

  public GraphEdgeJoinWithJoins(JoinType vertexJoinType, GeneralEdgeSemantics edgeSemanticsImplementation,
    DataSet<Edge> relations, @Nullable Function<Vertex, Function<Vertex, Boolean>> thetaVertex,
    @Nullable Function<GraphHead, Function<GraphHead, Boolean>> thetaGraph,
    @Nullable Function<Tuple2<String, String>, String> vertexLabelConcatenation,
    @Nullable Function<Tuple2<String, String>, String> graphLabelConcatenation) {
    super(vertexJoinType, edgeSemanticsImplementation,
      new PreFilterRelationalJoin(true, relations, vertexJoinType),
      new PreFilterRelationalJoin(false, relations, vertexJoinType), null, null, thetaVertex,
      thetaGraph,
      vertexLabelConcatenation, graphLabelConcatenation);
  }

}
