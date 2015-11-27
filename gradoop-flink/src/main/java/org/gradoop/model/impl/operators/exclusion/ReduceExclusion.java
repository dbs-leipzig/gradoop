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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.impl.operators.exclusion;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.impl.model.GraphCollection;
import org.gradoop.model.impl.model.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.model.impl.functions.graphcontainment.NotInGraphsBroadcast;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Takes a LogicalGraph specified by its id and removes all vertices and
 * edges, that are contained in other graphs of this collection. The result
 * is returned.
 *
 * @param <V> vertex data type
 * @param <E> edge data type
 * @param <G> graph data type
 */
public class ReduceExclusion
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToGraphOperator<G, V, E>{
  /**
   * ID defining the base graph
   */
  private final GradoopId positiveGraphID;

  /**
   * Create a new ReduceExclusion
   *
   * @param positiveGraphID ID defining the base graph, all vertices and
   *                        edges in this graph that
   *                        are also part of another graph in the
   *                        GraphCollection will be removed.
   *                        Its necessary to specify the first graph because
   *                        exclusion is not
   *                        commutative.
   */
  public ReduceExclusion(GradoopId positiveGraphID) {
    this.positiveGraphID = positiveGraphID;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(GraphCollection<G, V, E> collection) {

    DataSet<G> graphHeads = collection.getGraphHeads();

    DataSet<GradoopId> graphIDs = graphHeads
      .map(new Id<G>());

    graphIDs = graphIDs
      .filter(new RemoveGradoopIdFromDataSetFilter(positiveGraphID));

    DataSet<V> vertices = collection.getVertices()
      .filter(new InGraph<V>(positiveGraphID));

    vertices = vertices
      .filter(new NotInGraphsBroadcast<V>())
      .withBroadcastSet(
        graphIDs, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    DataSet<E> edges = collection.getEdges()
      .filter(new InGraph<E>(positiveGraphID));

    edges = edges
      .filter(new NotInGraphsBroadcast<E>())
      .withBroadcastSet(
        graphIDs, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    return LogicalGraph.fromDataSets(
      vertices,
      edges,
      collection.getConfig()
    );
  }

  @Override
  public String getName() {
    return ReduceExclusion.class.getName();
  }

  /**
   * Filter that removes
   */
  public static class RemoveGradoopIdFromDataSetFilter implements
    FilterFunction<GradoopId> {
    /**
     * Long that shall be removed from the DataSet
     */
    private GradoopId otherId;

    /**
     * Creates a filter
     *
     * @param otherId Long that shall be removed from the DataSet
     */
    public RemoveGradoopIdFromDataSetFilter(GradoopId otherId) {
      this.otherId = otherId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean filter(GradoopId longInSet) throws Exception {
      return !longInSet.equals(otherId);
    }
  }
}
