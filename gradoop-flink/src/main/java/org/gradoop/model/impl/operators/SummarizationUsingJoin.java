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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;

/**
 * Summarization implementation that uses join computations.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class SummarizationUsingJoin<VD extends VertexData, ED extends
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
  public SummarizationUsingJoin(String vertexGroupingKey,
    String edgeGroupingKey, boolean useVertexLabels, boolean useEdgeLabels) {
    super(vertexGroupingKey, edgeGroupingKey, useVertexLabels, useEdgeLabels);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Graph<Long, VD, ED> summarizeInternal(Graph<Long, VD, ED> graph) {
    /* build summarized vertices */
    SortedGrouping<Vertex<Long, VD>> groupedSortedVertices =
      groupAndSortVertices(graph);

    // create new summarized gelly vertices
    DataSet<Vertex<Long, VD>> newVertices =
      buildSummarizedVertices(groupedSortedVertices);

    // create mapping from vertex-id to group representative
    DataSet<Tuple2<Long, Long>> vertexToRepresentativeMap =
      groupedSortedVertices
        .reduceGroup(new VertexToRepresentativeReducer<VD>());

    /* build summarized vertices */
    DataSet<Edge<Long, ED>> newEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return Graph.fromDataSet(newVertices, newEdges, graph.getContext());
  }

  /**
   * Build summarized edges by joining them with vertices and their group
   * representative.
   *
   * @param graph                     inout graph
   * @param vertexToRepresentativeMap dataset containing tuples of vertex id
   *                                  and group representative
   * @return summarized edges
   */
  private DataSet<Edge<Long, ED>> buildSummarizedEdges(
    Graph<Long, VD, ED> graph,
    DataSet<Tuple2<Long, Long>> vertexToRepresentativeMap) {
    // join edges with vertex-group-map on vertex-id == edge-source-id
    DataSet<Tuple5<Long, Long, Long, String, String>> edges =
      graph.getEdges().join(vertexToRepresentativeMap).where(0).equalTo(0)
        // project edges to necessary information
        .with(new SourceVertexJoinFunction<ED>(getEdgeGroupingKey(),
          useEdgeLabels()))
          // join result with vertex-group-map on edge-target-id == vertex-id
        .join(vertexToRepresentativeMap).where(2).equalTo(0)
        .with(new TargetVertexJoinFunction());

    return groupEdges(edges).reduceGroup(
      new EdgeGroupSummarizer<>(getEdgeGroupingKey(), useEdgeLabels(),
        edgeDataFactory)).withForwardedFields("f0");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SummarizationUsingJoin.class.getName();
  }

  /**
   * Takes a group of vertex ids as input an emits a (vertex-id,
   * group-representative) tuple for each vertex in that group.
   * <p>
   * The group representative is the first vertex-id in the group.
   */
  private static class VertexToRepresentativeReducer<VD extends VertexData>
    implements
    GroupReduceFunction<Vertex<Long, VD>, Tuple2<Long, Long>> {

    /**
     * Avoid object instantiation in each reduce call.
     */
    private final Tuple2<Long, Long> reuseTuple;

    /**
     * Creates a group reduce function.
     */
    public VertexToRepresentativeReducer() {
      this.reuseTuple = new Tuple2<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<Vertex<Long, VD>> group,
      Collector<Tuple2<Long, Long>> collector) throws Exception {
      Long groupRepresentative = null;
      boolean first = true;
      for (Vertex<Long, VD> groupElement : group) {
        if (first) {
          groupRepresentative = groupElement.getId();
          first = false;
        }
        reuseTuple.f0 = groupElement.getId();
        reuseTuple.f1 = groupRepresentative;
        collector.collect(reuseTuple);
      }
    }
  }

  /**
   * Takes an edge and a tuple (vertex-id, group-representative) as input.
   * Replaces the edge-source-id with the group-representative and outputs
   * projected edge information possibly containing the edge label and a
   * group property.
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f1->f2") // edge target id
  @FunctionAnnotation.ForwardedFieldsSecond("f1") // edge source id
  private static class SourceVertexJoinFunction<ED extends EdgeData> implements
    JoinFunction<Edge<Long, ED>, Tuple2<Long, Long>, Tuple5<Long, Long, Long,
      String, String>> {

    /**
     * Vertex property key for grouping
     */
    private final String groupPropertyKey;
    /**
     * True if vertex label shall be used
     */
    private final boolean useLabel;

    /**
     * Avoid object initialization in each call.
     */
    private final Tuple5<Long, Long, Long, String, String> reuseTuple;

    /**
     * Creates join function.
     *
     * @param groupPropertyKey vertex property key for grouping
     * @param useLabel         true, if vertex label shall be used
     */
    public SourceVertexJoinFunction(String groupPropertyKey, boolean useLabel) {
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
      this.reuseTuple = new Tuple5<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple5<Long, Long, Long, String, String> join(Edge<Long, ED> e,
      Tuple2<Long, Long> vertexRepresentativeTuple) throws Exception {
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

      reuseTuple.f0 = e.getValue().getId();
      reuseTuple.f1 = vertexRepresentativeTuple.f1;
      reuseTuple.f2 = e.getTarget();
      reuseTuple.f3 = useLabel ? e.getValue().getLabel() : null;
      reuseTuple.f4 = groupingValue;

      return reuseTuple;
    }
  }

  /**
   * Takes a projected edge and an (vertex-id, group-representative) tuple
   * and replaces the edge-target-id with the group-representative.
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f0;f1;f3;f4")
  @FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
  private static class TargetVertexJoinFunction implements
    JoinFunction<Tuple5<Long, Long, Long, String, String>, Tuple2<Long,
      Long>, Tuple5<Long, Long, Long, String, String>> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple5<Long, Long, Long, String, String> join(
      Tuple5<Long, Long, Long, String, String> edge,
      Tuple2<Long, Long> vertexRepresentativeTuple) throws Exception {
      edge.setField(vertexRepresentativeTuple.f1, 2);
      return edge;
    }
  }
}
