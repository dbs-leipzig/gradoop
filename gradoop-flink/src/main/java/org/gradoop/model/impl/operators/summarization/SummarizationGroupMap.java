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

package org.gradoop.model.impl.operators.summarization;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;

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
public class SummarizationGroupMap<VD extends VertexData, ED extends
  EdgeData, GD extends GraphData> extends
  Summarization<VD, ED, GD> {
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
    // map vertex data to a smaller representation for grouping
    DataSet<VertexForGrouping> verticesForGrouping = graph.getVertices().map(
      new VertexDataToGroupVertexMapper<VD>(getVertexGroupingKey(),
        useVertexLabels()));
    // group vertices and build vertex group items
    DataSet<VertexGroupItem> groupedVertices =
      groupVertices(verticesForGrouping).reduceGroup(new VertexGroupReducer());

    // filter group representative tuples and build final vertices
    DataSet<Vertex<Long, VD>> summarizedVertices =
      groupedVertices.filter(new VertexGroupItemToSummarizedVertexFilter()).map(
        new VertexGroupItemToSummarizedVertexMapper<>(vertexDataFactory,
          getVertexGroupingKey(), useVertexLabels()));

    // filter vertex to representative tuples (used for edge join)
    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      groupedVertices.filter(new VertexGroupItemToRepresentativeFilter())
        .map(new VertexGroupItemToVertexWithRepresentativeMapper());

    // build summarized edges
    DataSet<Edge<Long, ED>> summarizedEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return Graph
      .fromDataSet(summarizedVertices, summarizedEdges, graph.getContext());
  }

  /**
   * Creates a single {@link VertexGroupItem} for each group element
   * containing the vertex id and the id of the group representative (the
   * first tuple in the input iterable.
   *
   * Creates one {@link VertexGroupItem} for the whole group that contains the
   * vertex id of the group representative, the group label, the group
   * property value and the total number of group elements.
   */
  @FunctionAnnotation.ForwardedFields("f0;f1->f2;f2->f3")
  private static class VertexGroupReducer implements
    GroupReduceFunction<VertexForGrouping, VertexGroupItem> {
    /**
     * Reduce object instantiations.
     */
    private final VertexGroupItem reuseVertexGroupItem;

    /**
     * Creates group reduce function.
     */
    private VertexGroupReducer() {
      reuseVertexGroupItem = new VertexGroupItem();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<VertexForGrouping> groupVertices,
      Collector<VertexGroupItem> collector) throws Exception {
      Long groupRepresentative = null;
      String groupLabel = null;
      String groupPropertyValue = null;
      boolean first = true;
      long groupCount = 0L;
      for (VertexForGrouping vertexForGrouping : groupVertices) {
        if (first) {
          groupRepresentative = vertexForGrouping.getVertexId();
          groupLabel = vertexForGrouping.getGroupLabel();
          groupPropertyValue = vertexForGrouping.getGroupPropertyValue();
          first = false;
        }
        // no need to set group label / property value for those tuples
        reuseVertexGroupItem.setVertexId(vertexForGrouping.getVertexId());
        reuseVertexGroupItem
          .setGroupRepresentativeVertexId(groupRepresentative);
        collector.collect(reuseVertexGroupItem);
        groupCount++;
      }

      createGroupRepresentativeTuple(groupRepresentative, groupLabel,
        groupPropertyValue, groupCount);
      collector.collect(reuseVertexGroupItem);
      reuseVertexGroupItem.reset();
    }

    /**
     * Creates one tuple representing the whole group. This tuple is later
     * used to create a summarized vertex for each group.
     *
     * @param groupRepresentative group representative vertex id
     * @param groupLabel          group label
     * @param groupPropertyValue  group property value
     * @param groupCount          total group count
     */
    private void createGroupRepresentativeTuple(Long groupRepresentative,
      String groupLabel, String groupPropertyValue, long groupCount) {
      reuseVertexGroupItem.setVertexId(groupRepresentative);
      reuseVertexGroupItem.setGroupLabel(groupLabel);
      reuseVertexGroupItem.setGroupPropertyValue(groupPropertyValue);
      reuseVertexGroupItem.setGroupCount(groupCount);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SummarizationGroupMap.class.getName();
  }
}
