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
package org.gradoop.flink.model.impl.operators.selection;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraphBroadcast;

/**
 * Superclass of selection and distinct operators.
 * Contains logic of vertex and edge selection and updating.
 */
public abstract class SelectionBase implements UnaryCollectionToCollectionOperator {

  @Override
  public abstract GraphCollection execute(GraphCollection collection);

  /**
   * Selects vertices and edges for a selected subset of graph heads / graph ids.
   * Creates a graph collection representing selection result.
   *
   * @param collection input collection
   * @param graphHeads selected graph heads
   *
   * @return selection result
   */
  protected GraphCollection selectVerticesAndEdges(
    GraphCollection collection, DataSet<GraphHead> graphHeads) {

    // get the identifiers of these logical graphs
    DataSet<GradoopId> graphIds = graphHeads.map(new Id<GraphHead>());

    // use graph ids to filter vertices from the actual graph structure
    DataSet<Vertex> vertices = collection.getVertices()
      .filter(new InAnyGraphBroadcast<>())
      .withBroadcastSet(graphIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    DataSet<Edge> edges = collection.getEdges()
      .filter(new InAnyGraphBroadcast<>())
      .withBroadcastSet(graphIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    return collection.getConfig().getGraphCollectionFactory()
      .fromDataSets(graphHeads, vertices, edges);
  }

  @Override
  public abstract String getName();
}
