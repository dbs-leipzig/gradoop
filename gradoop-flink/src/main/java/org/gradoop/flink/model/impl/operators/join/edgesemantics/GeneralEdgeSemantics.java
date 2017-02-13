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

package org.gradoop.flink.model.impl.operators.join.edgesemantics;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.JoinUtils;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.blocks.ConjunctiveFlatJoinFunction;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.blocks.DisjunctiveFlatJoinFunction;
import org.gradoop.flink.model.impl.operators.join.functions.Oplus;
import org.gradoop.flink.model.impl.operators.join.functions.OplusEdges;
import org.gradoop.flink.model.impl.operators.join.tuples.CombiningEdgeTuples;

import java.io.Serializable;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public class GeneralEdgeSemantics implements Serializable {
  public final JoinType edgeJoinType;
  public final FlatJoinFunction<CombiningEdgeTuples, CombiningEdgeTuples, Edge> joiner;

  public GeneralEdgeSemantics(JoinType edgeJoinType,
    FlatJoinFunction<CombiningEdgeTuples, CombiningEdgeTuples, Edge> joiner) {
    this.edgeJoinType = edgeJoinType;
    this.joiner = joiner;
  }

  public static GeneralEdgeSemantics fromEdgePredefinedSemantics(
    final Function<CombiningEdgeTuples, Function<CombiningEdgeTuples, Boolean>> thetaEdge,
    final PredefinedEdgeSemantics es,
    Function<Tuple2<String,String>,String> edgeLabelConcatenation) {
    OplusEdges combinateEdges = new OplusEdges(JoinUtils.generateConcatenator(edgeLabelConcatenation));
    return fromEdgePredefinedSemantics(thetaEdge, es, combinateEdges);
  }

  private static GeneralEdgeSemantics fromEdgePredefinedSemantics(
    final Function<CombiningEdgeTuples, Function<CombiningEdgeTuples, Boolean>> thetaEdge,
    final PredefinedEdgeSemantics es, final OplusEdges combineEdges) {
    JoinType edgeJoinType = null;
    FlatJoinFunction<CombiningEdgeTuples, CombiningEdgeTuples, Edge> joiner = null;
    final Function<Tuple2<CombiningEdgeTuples,CombiningEdgeTuples>,Boolean> finalThetaEdge =
      JoinUtils.extendBasic2(thetaEdge);
    switch (es) {
    case CONJUNCTIVE: {
      edgeJoinType = JoinType.INNER;
      joiner = new ConjunctiveFlatJoinFunction(finalThetaEdge,combineEdges);
    }
    break;
    case DISJUNCTIVE: {
      edgeJoinType = JoinType.FULL_OUTER;
      joiner = new DisjunctiveFlatJoinFunction(finalThetaEdge,combineEdges);
    }
    break;
    }
    return new GeneralEdgeSemantics(edgeJoinType, joiner);
  }

}
