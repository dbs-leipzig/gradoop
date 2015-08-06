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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.EdgeData;
import org.gradoop.model.VertexData;

import java.util.List;

public class SummarizationCross extends Summarization {

  SummarizationCross(String vertexGroupingKey, String edgeGroupingKey,
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

    // create a list of vertex ids from each vertex group
    DataSet<List<Long>> vertexGroups =
      createListFromVertexGroup(groupedSortedVertices);

    /* build summarized edges */
    DataSet<Edge<Long, EdgeData>> newEdges =
      buildSummarizedEdges(graph, vertexGroups);

    return Graph.fromDataSet(newVertices, newEdges, graph.getContext());
  }

  @Override
  public String getName() {
    return "SummarizationCross";
  }

  private DataSet<List<Long>> createListFromVertexGroup(
    SortedGrouping<Vertex<Long, VertexData>> groupedSortedVertices) {
    return groupedSortedVertices.reduceGroup(new VertexGroupToList());
  }

  private DataSet<Edge<Long, EdgeData>> buildSummarizedEdges(
    Graph<Long, VertexData, EdgeData> graph, DataSet<List<Long>> vertexGroups) {
    // map edges to relevant information
    // (edge id, source vertex, target vertex, edge label, grouping value)
    DataSet<Tuple5<Long, Long, Long, String, String>> edges = graph.getEdges()
      .map(new EdgeProjection(getEdgeGroupingKey(), useEdgeLabels()));

    // compute cross between vertex groups and edges
    DataSet<Tuple2<List<Long>, Tuple5<Long, Long, Long, String, String>>>
      groupEdgeCross = vertexGroups.cross(edges);

    // create intra edges and (possible incomplete) inter edges
    DataSet<Tuple5<Long, Long, Long, String, String>> firstRoundEdges =
      groupEdgeCross.flatMap(new FirstRoundFlatMap());

    /* build intra edges */

    FilterOperator<Tuple5<Long, Long, Long, String, String>>
      filteredIntraEdges = firstRoundEdges
      // filter intra edges (source == target)
      .filter(new IntraEdgeFilterWith());

    DataSet<Edge<Long, EdgeData>> intraEdges = groupEdges(filteredIntraEdges)
      // sort group by edge id to get edge representative
      .sortGroup(0, Order.ASCENDING)
        // and create new gelly edges with payload
      .reduceGroup(
        new EdgeGroupSummarizer(getEdgeGroupingKey(), useEdgeLabels()));

    /* build inter edges */

    DataSet<Tuple5<Long, Long, Long, String, String>> filteredInterEdges =
      firstRoundEdges
        // filter inter edge candidates (source != target)
        .filter(new InterEdgeFilter());

    // cross inter-edges candidates with vertex groups
    groupEdgeCross = vertexGroups.cross(filteredInterEdges);

    // replace target vertex with group representative if possible
    FlatMapOperator<Tuple2<List<Long>, Tuple5<Long, Long, Long, String,
      String>>, Tuple5<Long, Long, Long, String, String>>
      secondRoundEdges = groupEdgeCross
      // finalize inter-edges
      .flatMap(new SecondRoundFlatMap());

    // sort group by edge id to get edge representative
    DataSet<Edge<Long, EdgeData>> interEdges =
      groupEdges(secondRoundEdges).sortGroup(0, Order.ASCENDING)
        // and create new gelly edges with payload
        .reduceGroup(
          new EdgeGroupSummarizer(getEdgeGroupingKey(), useEdgeLabels()));

    return interEdges.union(intraEdges);
  }

  /**
   * Converts grouped gelly vertices to a list of vertex ids.
   */
  private static class VertexGroupToList implements
    GroupReduceFunction<Vertex<Long, VertexData>, List<Long>> {
    @Override
    public void reduce(Iterable<Vertex<Long, VertexData>> vertices,
      Collector<List<Long>> collector) throws Exception {
      List<Long> vertexIDs = Lists.newArrayList();
      for (Vertex<Long, VertexData> v : vertices) {
        vertexIDs.add(v.getId());
      }
      collector.collect(vertexIDs);
    }
  }

  /**
   * Creates intra/inter edges by replacing source-vertex [and target-vertex]
   * with their corresponding vertex group representative.
   * <p/>
   * Takes a tuple (vertex-group, edge) as input and returns a new edge
   * considering three options:
   * <p/>
   * 1)
   * source-vertex in group and target-vertex in group =>
   * (group-representative, group-representative) // intra-edge
   * <p/>
   * 2)
   * source-vertex in group =>
   * (group-representative, target-vertex) // inter-edge
   * <p/>
   * 3)
   * target-vertex in group =>
   * no output as this is processed by another group, edge pair
   */
  private static class FirstRoundFlatMap implements
    FlatMapFunction<Tuple2<List<Long>, Tuple5<Long, Long, Long, String,
      String>>, Tuple5<Long, Long, Long, String, String>> {

    @Override
    public void flatMap(
      Tuple2<List<Long>, Tuple5<Long, Long, Long, String, String>> t,
      Collector<Tuple5<Long, Long, Long, String, String>> coll) throws
      Exception {
      List<Long> sortedVertexGroup = t.f0;
      Tuple5<Long, Long, Long, String, String> edge = t.f1;
      Long edgeID = edge.f0;
      Long sourceVertex = edge.f1;
      Long targetVertex = edge.f2;
      String edgeLabel = edge.f3;
      String edgeGroupingValue = edge.f4;
      // list is sorted, representative is the first element
      Long groupRepresentative = sortedVertexGroup.get(0);
      // source vertex in group ?
      if (sortedVertexGroup.contains(sourceVertex)) {
        // target in vertex group ?
        if (sortedVertexGroup.contains(targetVertex)) {
          // create an intra edge
          coll.collect(
            new Tuple5<>(edgeID, groupRepresentative, groupRepresentative,
              edgeLabel, edgeGroupingValue));
        } else {
          // create an inter edge
          coll.collect(
            new Tuple5<>(edgeID, groupRepresentative, targetVertex, edgeLabel,
              edgeGroupingValue));
        }
      }
    }
  }

  /**
   * Creates final inter edges by replacing the target vertex with the
   * corresponding vertex group representative.
   */
  private static class SecondRoundFlatMap implements
    FlatMapFunction<Tuple2<List<Long>, Tuple5<Long, Long, Long, String,
      String>>, Tuple5<Long, Long, Long, String, String>> {

    @Override
    public void flatMap(
      Tuple2<List<Long>, Tuple5<Long, Long, Long, String, String>> t,
      Collector<Tuple5<Long, Long, Long, String, String>> coll) throws
      Exception {
      List<Long> sortedVertexGroup = t.f0;
      Tuple5<Long, Long, Long, String, String> edge = t.f1;
      Long edgeID = edge.f0;
      Long sourceVertex = edge.f1;
      Long targetVertex = edge.f2;
      String edgeLabel = edge.f3;
      String edgeGroupingValue = edge.f4;
      // list is sorted, representative is the first element
      Long groupRepresentative = sortedVertexGroup.get(0);

      // target vertex in group?
      if (sortedVertexGroup.contains(targetVertex)) {
        coll.collect(
          new Tuple5<>(edgeID, sourceVertex, groupRepresentative, edgeLabel,
            edgeGroupingValue));
      }
    }
  }

  /**
   * Filters intra edges (source-vertex == target-vertex).
   */
  private static class IntraEdgeFilterWith implements
    FilterFunction<Tuple5<Long, Long, Long, String, String>> {
    @Override
    public boolean filter(Tuple5<Long, Long, Long, String, String> t) throws
      Exception {
      return t.f1.equals(t.f2);
    }
  }

  /**
   * Filters inter edges (source-vertex != target-vertex).
   */
  private static class InterEdgeFilter implements
    FilterFunction<Tuple5<Long, Long, Long, String, String>> {

    @Override
    public boolean filter(Tuple5<Long, Long, Long, String, String> t) throws
      Exception {
      return !t.f1.equals(t.f2);
    }
  }

  /**
   * Reduces edges to the information which is relevant for further
   * processing (source vertex, target vertex, label and group value).
   */
  private static class EdgeProjection implements
    MapFunction<Edge<Long, EdgeData>, Tuple5<Long, Long, Long, String,
      String>> {

    private String groupPropertyKey;
    private boolean useLabel;

    public EdgeProjection(String groupPropertyKey, boolean useLabel) {
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
    }

    @Override
    public Tuple5<Long, Long, Long, String, String> map(
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

      return new Tuple5<>(e.getValue().getId(), e.getSource(), e.getTarget(),
        useLabel ? e.getValue().getLabel() : null, groupingValue);
    }
  }
}
