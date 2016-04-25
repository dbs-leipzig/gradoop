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

package org.gradoop.model.impl.operators.cloning;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Clone;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.PairElementWithNewId;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.epgm.TargetId;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.cloning.functions.EdgeSourceUpdateJoin;
import org.gradoop.model.impl.operators.cloning.functions.EdgeTargetUpdateJoin;
import org.gradoop.model.impl.operators.cloning.functions.Value0Of2ToId;
import org.gradoop.model.impl.operators.cloning.functions.ElementGraphUpdater;
import org.gradoop.model.impl.functions.epgm.ElementIdUpdater;

/**
 * Creates a copy of the logical graph with new ids for the graph head,
 * vertices and edges.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class Cloning
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    DataSet<G> graphHead = graph.getGraphHead().map(new Clone<G>());

    DataSet<GradoopId> graphId = graphHead.map(new Id<G>());

    //--------------------------------------------------------------------------
    // compute new vertices
    //--------------------------------------------------------------------------

    DataSet<Tuple2<V, GradoopId>> vertexTuple = graph.getVertices()
        .map(new PairElementWithNewId<V>());

    DataSet<Tuple2<GradoopId, GradoopId>> vertexIdTuple = vertexTuple
      .map(new Value0Of2ToId<V, GradoopId>());

    DataSet<V> vertices = vertexTuple
      .map(new ElementIdUpdater<V>())
      //update graph ids
      .map(new ElementGraphUpdater<V>())
      .withBroadcastSet(graphId, ElementGraphUpdater.GRAPHID);

    //--------------------------------------------------------------------------
    // compute new edges
    //--------------------------------------------------------------------------

    DataSet<E> edges = graph.getEdges()
      .map(new Clone<E>())
      //update source vertex ids
      .join(vertexIdTuple)
      .where(new SourceId<E>()).equalTo(0)
      .with(new EdgeSourceUpdateJoin<E>())
      //update target vertex ids
      .join(vertexIdTuple)
      .where(new TargetId<E>()).equalTo(0)
      .with(new EdgeTargetUpdateJoin<E>())
      //update graph ids
      .map(new ElementGraphUpdater<E>())
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
