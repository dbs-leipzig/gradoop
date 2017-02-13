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

package org.gradoop.flink.model.impl.operators.join;

import com.sun.istack.Nullable;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.PredefinedEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.tuples.CombiningEdgeTuples;

/**
 * Created by Giacomo Bergami on 31/01/17.
 */
public class GraphThetaJoin extends GeneralJoinPlan<Edge> {


  public GraphThetaJoin(JoinType vertexJoinType, GeneralEdgeSemantics edgeSemanticsImplementation) {
    super(vertexJoinType, edgeSemanticsImplementation, null, null, null, null, null,
      null, null, null);
  }

  public GraphThetaJoin(JoinType vertexJoinType, GeneralEdgeSemantics edgeSemanticsImplementation,
    @Nullable Function<Vertex, Long> leftHash, @Nullable Function<Vertex, Long> rightHash, @Nullable Function<Vertex, Function<Vertex, Boolean>> thetaVertex,
    @Nullable Function<GraphHead, Function<GraphHead, Boolean>> thetaGraph, @Nullable Function<Tuple2<String,String>,String>
    vertexLabelConcatenation,
    @Nullable Function<Tuple2<String,String>,String> graphLabelConcatenation) {
    super(vertexJoinType, edgeSemanticsImplementation, null, null, leftHash, rightHash, thetaVertex,
      thetaGraph, vertexLabelConcatenation, graphLabelConcatenation);
  }

  public GraphThetaJoin(JoinType vertexJoinType,
    final Function<CombiningEdgeTuples, Function<CombiningEdgeTuples, Boolean>> thetaEdge,
    final PredefinedEdgeSemantics es,
    Function<Tuple2<String,String>,String> edgeLabelConcatenation, @Nullable Function leftHash,
    @Nullable Function rightHash, @Nullable Function thetaVertex, @Nullable Function thetaGraph,
    @Nullable Function vertexLabelConcatenation, @Nullable Function graphLabelConcatenation) {
    super(vertexJoinType,
      GeneralEdgeSemantics.fromEdgePredefinedSemantics(thetaEdge, es, edgeLabelConcatenation), null,
      null, leftHash, rightHash, thetaVertex, thetaGraph, vertexLabelConcatenation,
      graphLabelConcatenation);
  }

}
