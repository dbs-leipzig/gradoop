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

package org.gradoop.flink.model.impl.operators.cloning;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ElementIdUpdater;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.cloning.functions
  .EdgeSourceUpdateJoin;
import org.gradoop.flink.model.impl.operators.cloning.functions.EdgeTargetUpdateJoin;
import org.gradoop.flink.model.impl.operators.cloning.functions.ElementGraphUpdater;
import org.gradoop.flink.model.impl.operators.cloning.functions.Value0Of2ToId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.functions.epgm.Clone;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.PairElementWithNewId;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates a copy of the logical graph with new ids for the graph head,
 * vertices and edges.
 */
public class Cloning implements UnaryGraphToGraphOperator {

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    DataSet<GraphHead> graphHead = graph.getGraphHead()
      .map(new Clone<GraphHead>());

    DataSet<GradoopId> graphId = graphHead.map(new Id<GraphHead>());

    //--------------------------------------------------------------------------
    // compute new vertices
    //--------------------------------------------------------------------------

    DataSet<Tuple2<Vertex, GradoopId>> vertexTuple = graph.getVertices()
        .map(new PairElementWithNewId<Vertex>());

    DataSet<Tuple2<GradoopId, GradoopId>> vertexIdTuple = vertexTuple
      .map(new Value0Of2ToId<Vertex, GradoopId>());

    DataSet<Vertex> vertices = vertexTuple
      .map(new ElementIdUpdater<Vertex>())
      //update graph ids
      .map(new ElementGraphUpdater<Vertex>())
      .withBroadcastSet(graphId, ElementGraphUpdater.GRAPHID);

    //--------------------------------------------------------------------------
    // compute new edges
    //--------------------------------------------------------------------------

    DataSet<Edge> edges = graph.getEdges()
      .map(new Clone<Edge>())
      //update source vertex ids
      .join(vertexIdTuple)
      .where(new SourceId<>()).equalTo(0)
      .with(new EdgeSourceUpdateJoin<>())
      //update target vertex ids
      .join(vertexIdTuple)
      .where(new TargetId<>()).equalTo(0)
      .with(new EdgeTargetUpdateJoin<>())
      //update graph ids
      .map(new ElementGraphUpdater<Edge>())
      .withBroadcastSet(graphId, ElementGraphUpdater.GRAPHID);

    return LogicalGraph.fromDataSets(graphHead,
      vertices, edges, graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Cloning.class.getName();
  }
}
