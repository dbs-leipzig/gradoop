/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.cloning;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Clone;
import org.gradoop.flink.model.impl.functions.epgm.ElementIdUpdater;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.PairElementWithNewId;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.cloning.functions.EdgeSourceUpdateJoin;
import org.gradoop.flink.model.impl.operators.cloning.functions.EdgeTargetUpdateJoin;
import org.gradoop.flink.model.impl.operators.cloning.functions.ElementGraphUpdater;
import org.gradoop.flink.model.impl.operators.cloning.functions.Value0Of2ToId;

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

    return graph.getConfig().getLogicalGraphFactory()
      .fromDataSets(graphHead, vertices, edges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Cloning.class.getName();
  }
}
