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
package org.gradoop.model.impl.operators.collection;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.filterfunctions.EdgeInGraphsFilter;
import org.gradoop.model.impl.functions.filterfunctions
  .EdgeInNoneOfGraphsFilterWithBC;
import org.gradoop.model.impl.functions.filterfunctions.VertexInGraphsFilter;
import org.gradoop.model.impl.functions.filterfunctions
  .VertexInNoneOfGraphsFilterWithBC;
import org.gradoop.model.impl.functions.mapfunctions.GraphToIdentifierMapper;
import org.gradoop.util.FlinkConstants;

import java.util.List;

/**
 * Takes a LogicalGraph specified by its id and removes all vertices and
 * edges, that are contained in other graphs of this collection. The result
 * is returned.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class ExcludeCollection<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> implements
  UnaryCollectionToGraphOperator<VD, ED, GD> {
  /**
   * ID defining the base graph
   */
  private final Long positiveGraphID;

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
  public ExcludeCollection(Long positiveGraphID) {
    this.positiveGraphID = positiveGraphID;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> execute(
    GraphCollection<VD, ED, GD> collection) {
    final List<Long> positiveIDs = Lists.newArrayList(positiveGraphID);
    DataSet<GD> graphHeads = collection.getGraphHeads();
    DataSet<Long> graphIDs = graphHeads.map(new GraphToIdentifierMapper<GD>());
    graphIDs =
      graphIDs.filter(new RemoveLongFromDataSetFilter(positiveGraphID));
    DataSet<VD> vertices = collection.getVertices()
      .filter(new VertexInGraphsFilter<VD>(positiveIDs));
    vertices = vertices.filter(new VertexInNoneOfGraphsFilterWithBC<VD>())
      .withBroadcastSet(graphIDs,
        VertexInNoneOfGraphsFilterWithBC.BC_IDENTIFIERS);
    DataSet<ED> edges =
      collection.getEdges().filter(new EdgeInGraphsFilter<ED>(positiveIDs));
    edges = edges.filter(new EdgeInNoneOfGraphsFilterWithBC<ED>())
      .withBroadcastSet(graphIDs,
        EdgeInNoneOfGraphsFilterWithBC.BC_IDENTIFIERS);
    return LogicalGraph.fromDataSets(vertices, edges,
      collection.getConfig().getGraphHeadFactory()
        .createGraphData(FlinkConstants.EXCLUDE_GRAPH_ID),
      collection.getConfig());
  }

  @Override
  public String getName() {
    return ExcludeCollection.class.getName();
  }

  /**
   * Filter that removes
   */
  public static class RemoveLongFromDataSetFilter implements
    FilterFunction<Long> {
    /**
     * Long that shall be removed from the DataSet
     */
    private Long otherLong;

    /**
     * Creates a filter
     *
     * @param otherLong Long that shall be removed from the DataSet
     */
    public RemoveLongFromDataSetFilter(Long otherLong) {
      this.otherLong = otherLong;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean filter(Long longInSet) throws Exception {
      return !longInSet.equals(otherLong);
    }
  }
}
