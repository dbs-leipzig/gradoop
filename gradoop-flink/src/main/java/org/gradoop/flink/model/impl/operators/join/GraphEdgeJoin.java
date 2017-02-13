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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.operators.PreFilter;

/**
 * Created by Giacomo Bergami on 31/01/17.
 */
public class GraphEdgeJoin extends GeneralJoinPlan<GradoopId> {

  private static final Function<Boolean, Function<DataSet<Edge>, PreFilter<Vertex, GradoopId>>>
    relationalJoinPrefilter =
    (Boolean isLeft) -> (DataSet<Edge> e) -> (PreFilter<Vertex, GradoopId>) vertexDataSet ->
      vertexDataSet
      .join(e).where((Vertex v) -> v.getId())
      .equalTo((Edge e1) -> isLeft ? e1.getSourceId() : e1.getTargetId())
      .with(new JoinFunction<Vertex, Edge, Tuple2<Vertex, GradoopId>>() {
        @Override
        public Tuple2<Vertex, GradoopId> join(Vertex first, Edge second) throws Exception {
          return new Tuple2<>(first, second.getId());
        }
      }).returns(TypeInfoParser.parse(
        Tuple2.class.getName() + "<" + Vertex.class.getCanonicalName() + "," +
          GradoopId.class.getCanonicalName() + ">"));
  private final DataSet<Edge> relations;


  public GraphEdgeJoin(JoinType vertexJoinType, GeneralEdgeSemantics edgeSemanticsImplementation,
    DataSet<Edge> relations, @Nullable Function<Vertex, Function<Vertex, Boolean>> thetaVertex,
    @Nullable Function<GraphHead, Function<GraphHead, Boolean>> thetaGraph,
    @Nullable Function<Tuple2<String,String>,String> vertexLabelConcatenation,
    @Nullable Function<Tuple2<String,String>,String> graphLabelConcatenation) {
    super(vertexJoinType, edgeSemanticsImplementation,
      relationalJoinPrefilter.apply(true).apply(relations),
      relationalJoinPrefilter.apply(false).apply(relations), null, null, thetaVertex, thetaGraph,
      vertexLabelConcatenation, graphLabelConcatenation);
    this.relations = relations;
  }

}
