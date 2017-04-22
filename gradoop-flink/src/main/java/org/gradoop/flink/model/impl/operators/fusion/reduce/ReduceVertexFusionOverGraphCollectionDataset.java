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

package org.gradoop.flink.model.impl.operators.fusion.reduce;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.GraphCollectionGraphCollectionToGraph;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphsBroadcast;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.AddVerticesToNewGraph;

import static org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast.GRAPH_IDS;

/**
 * Given a graph collection of data and a graph collection representing hypervertices,
 * combines the data into a datalake and, for each graph in the graph collection
 * representing a final hypervertex, fuses each set of vertices as a single hypervertex
 *
 */
public class ReduceVertexFusionOverGraphCollectionDataset
  extends ReduceVertexFusionOverBinaryGraphs
  implements GraphCollectionGraphCollectionToGraph {

  /**
   * Multi-way join between the elements
   * @param dataCollection  Collection of graph containing vertices that have to be fused
   * @param hypervertices   Defining which elements are going to be fused
   * @return                Fused data lake
   */
  @Override
  public LogicalGraph execute(GraphCollection dataCollection,
    GraphCollection hypervertices) {
    LogicalGraph gU = new ReduceCombination().execute(dataCollection);

    DataSet<Vertex> vertices = hypervertices.getVertices()
      .filter(new InGraphsBroadcast<>())
      .withBroadcastSet(dataCollection.getGraphHeads().map(new Id<>()), GRAPH_IDS)
      .crossWithTiny(gU.getGraphHead())
      .with(new AddVerticesToNewGraph());

    GraphCollection updatedEntries = GraphCollection.fromDataSets(hypervertices.getGraphHeads(),
      vertices,
      hypervertices.getEdges(), hypervertices.getConfig());

    return execute(gU, updatedEntries);
  }

  @Override
  public String getName() {
    return ReduceVertexFusionOverGraphCollectionDataset.class.getName();
  }
}
