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

package org.gradoop.model.impl.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.EdgeData;
import org.gradoop.model.VertexData;

public class SummarizationJoin extends Summarization {
  SummarizationJoin(String vertexGroupingKey, String edgeGroupingKey,
    boolean useVertexLabels, boolean useEdgeLabels) {
    super(vertexGroupingKey, edgeGroupingKey, useVertexLabels, useEdgeLabels);
  }

  @Override
  protected Graph<Long, VertexData, EdgeData> summarizeInternal(
    Graph<Long, VertexData, EdgeData> graph) {

    /* build summarized vertices */
    SortedGrouping<Vertex<Long, VertexData>> groupedSortedVertices =
      groupAndSortVertices(graph);

    // create new summarized gelly vertices
    DataSet<Vertex<Long, VertexData>> newVertices =
      buildSummarizedVertices(groupedSortedVertices);

    // create mapping from vertex-id to group representative
    DataSet<Tuple2<Long, Long>> vertexToRepresentativeMap =
      groupedSortedVertices.reduceGroup(new VertexToRepresentativeReducer());

    /* build summarized vertices */
    DataSet<Edge<Long, EdgeData>> newEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return Graph.fromDataSet(newVertices, newEdges, graph.getContext());
  }

  private DataSet<Edge<Long, EdgeData>> buildSummarizedEdges(
    Graph<Long, VertexData, EdgeData> graph,
    DataSet<Tuple2<Long, Long>> vertexToRepresentativeMap) {
    // join vertex-group-map with edges on vertex-id == edge-source-id
    DataSet<Tuple5<Long, Long, Long, String, String>> edges =
      vertexToRepresentativeMap.join(graph.getEdges()).where(0).equalTo(0)
        // project edges to necessary information
        .with(
          new SourceVertexJoinFunction(getEdgeGroupingKey(), useEdgeLabels()))
          // join result with vertex-group-map on edge-target-id == vertex-id
        .join(vertexToRepresentativeMap).where(2).equalTo(0)
        .with(new TargetVertexJoinFunction());

    // sort group by edge id to get edge group representative (smallest id)
    return groupEdges(edges).sortGroup(0, Order.ASCENDING).reduceGroup(
      new EdgeGroupSummarizer(getEdgeGroupingKey(), useEdgeLabels()));
  }

  @Override
  public String getName() {
    return "SummarizationJoin";
  }

  /**
   * Takes a group of vertex ids as input an emits a (vertex-id,
   * group-representative) tuple for each vertex in that group.
   *
   * The group representative is the first vertex-id in the group.
   */
  private static class VertexToRepresentativeReducer implements
    GroupReduceFunction<Vertex<Long, VertexData>, Tuple2<Long, Long>> {

    public void reduce(Iterable<Vertex<Long, VertexData>> group,
      Collector<Tuple2<Long, Long>> collector) throws Exception {
      Long groupRepresentative = null;
      boolean first = true;
      for (Vertex<Long, VertexData> groupElement : group) {
        if (first) {
          groupRepresentative = groupElement.getId();
          first = false;
        }
        collector
          .collect(new Tuple2<>(groupElement.getId(), groupRepresentative));
      }
    }
  }

  /**
   * Takes a tuple (vertex-id, group-representative) and an edge as input.
   * Replaces the edge-source-id with the group-representative and outputs
   * projected edge information possibly containing the edge label and a
   * group property.
   */
  private static class SourceVertexJoinFunction implements
    JoinFunction<Tuple2<Long, Long>, Edge<Long, EdgeData>,
      Tuple5<Long, Long, Long, String, String>> {

    private final String groupPropertyKey;
    private final boolean useLabel;

    public SourceVertexJoinFunction(String groupPropertyKey, boolean useLabel) {
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
    }

    @Override
    public Tuple5<Long, Long, Long, String, String> join(
      Tuple2<Long, Long> vertexRepresentativeTuple,
      Edge<Long, EdgeData> e) throws Exception {
      String groupingValue = null;
      boolean useProperty =
        groupPropertyKey != null && !"".equals(groupPropertyKey);
      boolean hasProperty =
        useProperty && (e.getValue().getProperty(groupPropertyKey) != null);

      if (useProperty && hasProperty) {
        groupingValue = e.getValue().getProperty(groupPropertyKey).toString();
      } else if (useProperty) {
        groupingValue = NULL_VALUE;
      }

      return new Tuple5<>(e.getValue().getId(), vertexRepresentativeTuple.f1,
        e.getTarget(), (useLabel) ? e.getValue().getLabel() : null,
        groupingValue);
    }
  }

  /**
   * Takes a projected edge and an (vertex-id, group-representative) tuple
   * and replaces the edge-target-id with the group-representative.
   */
  private static class TargetVertexJoinFunction implements
    JoinFunction<Tuple5<Long, Long, Long, String, String>, Tuple2<Long,
      Long>, Tuple5<Long, Long, Long, String, String>> {

    @Override
    public Tuple5<Long, Long, Long, String, String> join(
      Tuple5<Long, Long, Long, String, String> edge,
      Tuple2<Long, Long> vertexRepresentativeTuple) throws Exception {
      edge.setField(vertexRepresentativeTuple.f1, 2);
      return edge;
    }
  }
}
