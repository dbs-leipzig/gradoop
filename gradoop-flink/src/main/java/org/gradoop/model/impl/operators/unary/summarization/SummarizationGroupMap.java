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

package org.gradoop.model.impl.operators.unary.summarization;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.operators.unary.summarization.functions
  .VertexDataToGroupVertexMapper;
import org.gradoop.model.impl.operators.unary.summarization.functions
  .VertexGroupItemToRepresentativeFilter;
import org.gradoop.model.impl.operators.unary.summarization.functions
  .VertexGroupItemToSummarizedVertexFilter;
import org.gradoop.model.impl.operators.unary.summarization.functions
  .VertexGroupItemToSummarizedVertexMapper;
import org.gradoop.model.impl.operators.unary.summarization.functions
  .VertexGroupItemToVertexWithRepresentativeMapper;
import org.gradoop.model.impl.operators.unary.summarization.functions
  .VertexGroupReducer;
import org.gradoop.model.impl.operators.unary.summarization.tuples.VertexForGrouping;
import org.gradoop.model.impl.operators.unary.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.unary.summarization.tuples
  .VertexWithRepresentative;

/**
 * Summarization implementation that does not require sorting of vertex groups.
 *
 * Algorithmic idea:
 *
 * 1) Map vertices to a smaller representation {@link org.gradoop.model.impl
 * .operators.summarization.SummarizationGroupMap.VertexForGrouping}
 * 2) Group vertices on label and/or property.
 * 3) Reduce group and collect one {@link VertexGroupItem} for each group
 * element and one additional {@link VertexGroupItem} for the group that
 * holds the group count.
 * 4) Filter output of 3
 * a) tuples with group count == 0 are mapped to {@link
 * VertexWithRepresentative}
 * b) tuples with group count > 0 are used to build final summarized vertices
 * 5) Output of 4a) is joined with edges
 * 6) Edge source and target vertex ids are replaced by group representative.
 * 7) Edges are grouped by source and target id and optionally by label
 * and/or edge property.
 * 8) Group reduce edges and create final summarized edges.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class SummarizationGroupMap<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData> extends Summarization<VD, ED, GD> {
  /**
   * Creates summarization.
   *
   * @param vertexGroupingKey property key to summarize vertices
   * @param edgeGroupingKey   property key to summarize edges
   * @param useVertexLabels   summarize on vertex label true/false
   * @param useEdgeLabels     summarize on edge label true/false
   */
  public SummarizationGroupMap(String vertexGroupingKey, String edgeGroupingKey,
    boolean useVertexLabels, boolean useEdgeLabels) {
    super(vertexGroupingKey, edgeGroupingKey, useVertexLabels, useEdgeLabels);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Graph<Long, VD, ED> summarizeInternal(Graph<Long, VD, ED> graph) {
    DataSet<VertexForGrouping> verticesForGrouping = graph.getVertices()
      // map vertex data to a smaller representation for grouping
      .map(new VertexDataToGroupVertexMapper<VD>(getVertexGroupingKey(),
        useVertexLabels()));

    DataSet<VertexGroupItem> groupedVertices =
      // group vertices by label / property / both
      groupVertices(verticesForGrouping)
        // build vertex group item
        .reduceGroup(new VertexGroupReducer());

    DataSet<Vertex<Long, VD>> summarizedVertices = groupedVertices
      // filter group representative tuples
      .filter(new VertexGroupItemToSummarizedVertexFilter())
      // build summarized vertex
      .map(new VertexGroupItemToSummarizedVertexMapper<>(vertexDataFactory,
        getVertexGroupingKey(), useVertexLabels()));

    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      groupedVertices
        // filter group element tuples
        .filter(new VertexGroupItemToRepresentativeFilter())
        // build vertex to group representative tuple
        .map(new VertexGroupItemToVertexWithRepresentativeMapper());

    // build summarized edges
    DataSet<Edge<Long, ED>> summarizedEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return Graph
      .fromDataSet(summarizedVertices, summarizedEdges, graph.getContext());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SummarizationGroupMap.class.getName();
  }
}
