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
package org.gradoop.model.impl.operators.collection.unary;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.filterfunctions.EdgeInGraphsFilter;
import org.gradoop.model.impl.functions.filterfunctions
  .EdgeInNoneOfGraphsFilterWithBC;
import org.gradoop.model.impl.functions.filterfunctions.VertexInGraphsFilter;
import org.gradoop.model.impl.functions.filterfunctions
  .VertexInNoneOfGraphsFilterWithBC;
import org.gradoop.model.impl.functions.isolation.ElementIdOnly;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Takes a LogicalGraph specified by its id and removes all vertices and
 * edges, that are contained in other graphs of this collection. The result
 * is returned.
 *
 * @param <V> vertex data type
 * @param <E> edge data type
 * @param <G> graph data type
 */
public class ExcludeCollection
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToGraphOperator<G, V, E>{
  /**
   * ID defining the base graph
   */
  private final GradoopId positiveGraphID;

  /**
   * Create a new ExcludeCollection
   *
   * @param positiveGraphID ID defining the base graph, all vertices and
   *                        edges in this graph that
   *                        are also part of another graph in the
   *                        GraphCollection will be removed.
   *                        Its necessary to specify the first graph because
   *                        exclusion is not
   *                        commutative.
   */
  public ExcludeCollection(GradoopId positiveGraphID) {
    this.positiveGraphID = positiveGraphID;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(GraphCollection<G, V, E> collection) {

    final GradoopIdSet positiveGraphIDs =
      GradoopIdSet.fromExisting(positiveGraphID);

    DataSet<G> graphHeads = collection.getGraphHeads();

    DataSet<GradoopId> graphIDs = graphHeads
      .map(new ElementIdOnly<G>());

    graphIDs = graphIDs
      .filter(new RemoveGradoopIdFromDataSetFilter(positiveGraphID));

    DataSet<V> vertices = collection.getVertices()
      .filter(new VertexInGraphsFilter<V>(positiveGraphIDs));

    vertices = vertices
      .filter(new VertexInNoneOfGraphsFilterWithBC<V>())
      .withBroadcastSet(
        graphIDs, VertexInNoneOfGraphsFilterWithBC.BC_IDENTIFIERS);

    DataSet<E> edges = collection.getEdges()
      .filter(new EdgeInGraphsFilter<E>(positiveGraphIDs));

    edges = edges
      .filter(new EdgeInNoneOfGraphsFilterWithBC<E>())
      .withBroadcastSet(
        graphIDs, EdgeInNoneOfGraphsFilterWithBC.BC_IDENTIFIERS);

    return LogicalGraph.fromDataSets(
      vertices,
      edges,
      collection.getConfig()
    );
  }

  @Override
  public String getName() {
    return ExcludeCollection.class.getName();
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
