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
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkVertexData;

import java.util.List;

import static org.gradoop.model.impl.EPGraph.VERTEX_ID;

/**
 * The summarization operator determines a structural grouping of similar
 * vertices and edges to condense a graph and thus help to uncover insights
 * about patterns hidden in the graph.
 * <p>
 * The graph summarization operator represents every vertex group by a single
 * vertex in the summarized graph; edges between vertices in the summary graph
 * represent a group of edges between the vertex group members of the
 * original graph. Summarization is defined by specifying grouping keys for
 * vertices and edges, respectively, similarly as for GROUP BY in SQL.
 * <p>
 * Consider the following example:
 * <p>
 * Input graph:
 * <p>
 * Vertices:<br>
 * (0, "Person", {city: L})<br>
 * (1, "Person", {city: L})<br>
 * (2, "Person", {city: D})<br>
 * (3, "Person", {city: D})<br>
 * <p>
 * Edges:{(0,1), (1,0), (1,2), (2,1), (2,3), (3,2)}
 * <p>
 * Output graph (summarized on vertex property "city"):
 * <p>
 * Vertices:<br>
 * (0, "Person", {city: L, count: 2})
 * (2, "Person", {city: D, count: 2})
 * <p>
 * Edges:<br>
 * ((0, 0), {count: 2}) // 2 intra-edges in L<br>
 * ((2, 2), {count: 2}) // 2 intra-edges in L<br>
 * ((0, 2), {count: 1}) // 1 inter-edge from L to D<br>
 * ((2, 0), {count: 1}) // 1 inter-edge from D to L<br>
 *
 * @author Martin Junghanns
 */
public class SummarizationCross extends Summarization {

  SummarizationCross(String vertexGroupingKey, String edgeGroupingKey,
    boolean useVertexLabels, boolean useEdgeLabels) {
    super(vertexGroupingKey, edgeGroupingKey, useVertexLabels, useEdgeLabels);
  }

  @Override
  protected Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> summarizeInternal(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph) {
    /* build summarized vertices */

    SortedGrouping<Vertex<Long, EPFlinkVertexData>> groupedSortedVertices =
      groupAndSortVertices(graph);

    // create new summarized gelly vertices
    DataSet<Vertex<Long, EPFlinkVertexData>> newVertices =
      buildSummarizedVertices(groupedSortedVertices);

    // create a list of vertex ids from each vertex group
    DataSet<List<Long>> vertexGroups =
      createListFromVertexGroup(groupedSortedVertices);

    /* build summarized edges */
    DataSet<Edge<Long, EPFlinkEdgeData>> newEdges =
      buildSummarizedEdges(graph, vertexGroups);

    return Graph.fromDataSet(newVertices, newEdges, graph.getContext());
  }

  @Override
  public String getName() {
    return "SummarizationCross";
  }

  private DataSet<List<Long>> createListFromVertexGroup(
    SortedGrouping<Vertex<Long, EPFlinkVertexData>> groupedSortedVertices) {
    return groupedSortedVertices.reduceGroup(new VertexGroupToList());
  }

  private DataSet<Edge<Long, EPFlinkEdgeData>> buildSummarizedEdges(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph,
    DataSet<List<Long>> vertexGroups) {
    if (edgeGroupingKey != null && !"".equals(edgeGroupingKey)) {
      return buildSummarizedEdgesWithGrouping(graph, vertexGroups);
    } else {
      return buildSummarizedEdgesWithoutGrouping(graph, vertexGroups);
    }
  }

  private DataSet<Edge<Long, EPFlinkEdgeData>>
  buildSummarizedEdgesWithoutGrouping(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph,
    DataSet<List<Long>> vertexGroups) {
    // map edges to relevant information (source vertex, target vertex)
    DataSet<Tuple3<Long, Long, Long>> edges =
      graph.getEdges().map(new EdgeProjection());

    // compute cross between vertex groups and edges
    DataSet<Tuple2<List<Long>, Tuple3<Long, Long, Long>>> groupEdgeCross =
      vertexGroups.cross(edges);

    // create intra edges and (possible incomplete) inter edges
    DataSet<Tuple3<Long, Long, Long>> firstRoundEdges =
      groupEdgeCross.flatMap(new FirstRoundFlatMap());

    /* build intra edges */

    DataSet<Edge<Long, EPFlinkEdgeData>> intraEdges = firstRoundEdges
      // filter intra edges (source == target)
      .filter(new IntraEdgeFilter())
        // group them by source vertex
      .groupBy(1)
        // sort group by edge id to get edge representative
      .sortGroup(0, Order.ASCENDING)
        // and create new gelly edges with payload
      .reduceGroup(new EdgeGroupSummarizer());

    /* build inter edges */

    DataSet<Tuple3<Long, Long, Long>> secondRoundEdges = firstRoundEdges
      // filter inter edge candidates (source != target)
      .filter(new InterEdgeFilter());

    // cross inter-edges candidates with vertex groups
    groupEdgeCross = vertexGroups.cross(secondRoundEdges);

    // replace target vertex with group representative if possible
    DataSet<Edge<Long, EPFlinkEdgeData>> interEdges = groupEdgeCross
      // finalize inter-edges
      .flatMap(new SecondRoundFlatMap())
        // group them by source and target vertex
      .groupBy(1, 2)
        // sort group by edge id to get edge representative
      .sortGroup(0, Order.ASCENDING)
        // and create new gelly edges with payload
      .reduceGroup(new EdgeGroupSummarizer());

    return interEdges.union(intraEdges);
  }

  private DataSet<Edge<Long, EPFlinkEdgeData>> buildSummarizedEdgesWithGrouping(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph,
    DataSet<List<Long>> vertexGroups) {
    // map edges to relevant information
    // (source vertex, target vertex, grouping value)
    DataSet<Tuple4<Long, Long, Long, String>> edges =
      graph.getEdges().map(new EdgeGroupProjection(edgeGroupingKey));

    // compute cross between vertex groups and edges
    DataSet<Tuple2<List<Long>, Tuple4<Long, Long, Long, String>>>
      groupEdgeCross = vertexGroups.cross(edges);

    // create intra edges and (possible incomplete) inter edges
    DataSet<Tuple4<Long, Long, Long, String>> firstRoundEdges =
      groupEdgeCross.flatMap(new FirstRoundFlatMapWithGroupValue());

    /* build intra edges */

    DataSet<Edge<Long, EPFlinkEdgeData>> intraEdges = firstRoundEdges
      // filter intra edges (source == target)
      .filter(new IntraEdgeFilterWithGroupValue())
        // group them by source vertex and group value
      .groupBy(1, 3)
        // sort group by edge id to get edge representative
      .sortGroup(0, Order.ASCENDING)
        // and create new gelly edges with payload
      .reduceGroup(new EdgeGroupSummarizerWithGroupValue(edgeGroupingKey));

    /* build inter edges */

    DataSet<Tuple4<Long, Long, Long, String>> secondRoundEdges = firstRoundEdges
      // filter inter edge candidates (source != target)
      .filter(new InterEdgeFilterWithGroupValue());

    // cross inter-edges candidates with vertex groups
    groupEdgeCross = vertexGroups.cross(secondRoundEdges);

    // replace target vertex with group representative if possible
    DataSet<Edge<Long, EPFlinkEdgeData>> interEdges = groupEdgeCross
      // finalize inter-edges
      .flatMap(new SecondRoundFlatMapWithGroupValue())
        // group them by source and target vertex and group value
      .groupBy(1, 2, 3)
        // sort group by edge id to get edge representative
      .sortGroup(0, Order.ASCENDING)
        // and create new gelly edges with payload
      .reduceGroup(new EdgeGroupSummarizerWithGroupValue(edgeGroupingKey));

    return interEdges.union(intraEdges);
  }

  /**
   * Converts grouped gelly vertices to a list of vertex ids.
   */
  private static class VertexGroupToList implements
    GroupReduceFunction<Vertex<Long, EPFlinkVertexData>, List<Long>> {
    @Override
    public void reduce(Iterable<Vertex<Long, EPFlinkVertexData>> iterable,
      Collector<List<Long>> collector) throws Exception {
      List<Long> vertexIDs = Lists.newArrayList();
      for (Vertex<Long, EPFlinkVertexData> v : iterable) {
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
    FlatMapFunction<Tuple2<List<Long>, Tuple3<Long, Long, Long>>,
      Tuple3<Long, Long, Long>> {

    @Override
    public void flatMap(Tuple2<List<Long>, Tuple3<Long, Long, Long>> t,
      Collector<Tuple3<Long, Long, Long>> coll) throws Exception {
      List<Long> sortedVertexGroup = t.f0;
      Tuple3<Long, Long, Long> edge = t.f1;
      Long edgeID = edge.f0;
      Long sourceVertex = edge.f1;
      Long targetVertex = edge.f2;
      // list is sorted, representative is the first element
      Long groupRepresentative = sortedVertexGroup.get(0);
      // source vertex in group ?
      if (sortedVertexGroup.contains(sourceVertex)) {
        // target in vertex group ?
        if (sortedVertexGroup.contains(targetVertex)) {
          // create an intra edge
          coll.collect(
            new Tuple3<>(edgeID, groupRepresentative, groupRepresentative));
        } else {
          // create an inter edge
          coll.collect(new Tuple3<>(edgeID, groupRepresentative, targetVertex));
        }
      }
    }
  }

  private static class FirstRoundFlatMapWithGroupValue implements
    FlatMapFunction<Tuple2<List<Long>, Tuple4<Long, Long, Long, String>>,
      Tuple4<Long, Long, Long, String>> {

    @Override
    public void flatMap(Tuple2<List<Long>, Tuple4<Long, Long, Long, String>> t,
      Collector<Tuple4<Long, Long, Long, String>> coll) throws Exception {
      List<Long> sortedVertexGroup = t.f0;
      Tuple4<Long, Long, Long, String> edge = t.f1;
      Long edgeID = edge.f0;
      Long sourceVertex = edge.f1;
      Long targetVertex = edge.f2;
      String edgeGroupingValue = edge.f3;
      // list is sorted, representative is the first element
      Long groupRepresentative = sortedVertexGroup.get(0);
      // source vertex in group ?
      if (sortedVertexGroup.contains(sourceVertex)) {
        // target in vertex group ?
        if (sortedVertexGroup.contains(targetVertex)) {
          // create an intra edge
          coll.collect(
            new Tuple4<>(edgeID, groupRepresentative, groupRepresentative,
              edgeGroupingValue));
        } else {
          // create an inter edge
          coll.collect(new Tuple4<>(edgeID, groupRepresentative, targetVertex,
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
    FlatMapFunction<Tuple2<List<Long>, Tuple3<Long, Long, Long>>,
      Tuple3<Long, Long, Long>> {

    @Override
    public void flatMap(Tuple2<List<Long>, Tuple3<Long, Long, Long>> t,
      Collector<Tuple3<Long, Long, Long>> coll) throws Exception {
      List<Long> sortedVertexGroup = t.f0;
      Tuple3<Long, Long, Long> edge = t.f1;
      Long edgeID = edge.f0;
      Long sourceVertex = edge.f1;
      Long targetVertex = edge.f2;
      // list is sorted, representative is the first element
      Long groupRepresentative = sortedVertexGroup.get(0);

      // target vertex in group?
      if (sortedVertexGroup.contains(targetVertex)) {
        coll.collect(new Tuple3<>(edgeID, sourceVertex, groupRepresentative));
      }
    }
  }

  /**
   * Creates final inter edges by replacing the target vertex with the
   * corresponding vertex group representative.
   */
  private static class SecondRoundFlatMapWithGroupValue implements
    FlatMapFunction<Tuple2<List<Long>, Tuple4<Long, Long, Long, String>>,
      Tuple4<Long, Long, Long, String>> {

    @Override
    public void flatMap(Tuple2<List<Long>, Tuple4<Long, Long, Long, String>> t,
      Collector<Tuple4<Long, Long, Long, String>> coll) throws Exception {
      List<Long> sortedVertexGroup = t.f0;
      Tuple4<Long, Long, Long, String> edge = t.f1;
      Long edgeID = edge.f0;
      Long sourceVertex = edge.f1;
      Long targetVertex = edge.f2;
      String edgeGroupingValue = edge.f3;
      // list is sorted, representative is the first element
      Long groupRepresentative = sortedVertexGroup.get(0);

      // target vertex in group?
      if (sortedVertexGroup.contains(targetVertex)) {
        coll.collect(new Tuple4<>(edgeID, sourceVertex, groupRepresentative,
          edgeGroupingValue));
      }
    }
  }

  /**
   * Filters intra edges (source-vertex == target-vertex).
   */
  private static class IntraEdgeFilter implements
    FilterFunction<Tuple3<Long, Long, Long>> {
    @Override
    public boolean filter(Tuple3<Long, Long, Long> t) throws Exception {
      return t.f1.equals(t.f2);
    }
  }

  /**
   * Filters intra edges (source-vertex == target-vertex).
   */
  private static class IntraEdgeFilterWithGroupValue implements
    FilterFunction<Tuple4<Long, Long, Long, String>> {
    @Override
    public boolean filter(Tuple4<Long, Long, Long, String> t) throws Exception {
      return t.f1.equals(t.f2);
    }
  }

  /**
   * Filters inter edges (source-vertex != target-vertex).
   */
  private static class InterEdgeFilter implements
    FilterFunction<Tuple3<Long, Long, Long>> {

    @Override
    public boolean filter(Tuple3<Long, Long, Long> t) throws Exception {
      return !t.f1.equals(t.f2);
    }
  }

  /**
   * Filters inter edges (source-vertex != target-vertex).
   */
  private static class InterEdgeFilterWithGroupValue implements
    FilterFunction<Tuple4<Long, Long, Long, String>> {

    @Override
    public boolean filter(Tuple4<Long, Long, Long, String> t) throws Exception {
      return !t.f1.equals(t.f2);
    }
  }

  /**
   * Reduces edges to the information which is relevant for further
   * processing (source vertex and target vertex).
   */
  private static class EdgeProjection implements
    MapFunction<Edge<Long, EPFlinkEdgeData>, Tuple3<Long, Long, Long>> {
    @Override
    public Tuple3<Long, Long, Long> map(Edge<Long, EPFlinkEdgeData> e) throws
      Exception {
      return new Tuple3<>(e.getValue().getId(), e.getSource(), e.getTarget());
    }
  }

  /**
   * Reduces edges to the information which is relevant for further
   * processing (source vertex, target vertex and group value).
   */
  private static class EdgeGroupProjection implements
    MapFunction<Edge<Long, EPFlinkEdgeData>, Tuple4<Long, Long, Long, String>> {

    private String edgeGroupingKey;

    public EdgeGroupProjection(String edgeGroupingKey) {
      this.edgeGroupingKey = edgeGroupingKey;
    }

    @Override
    public Tuple4<Long, Long, Long, String> map(
      Edge<Long, EPFlinkEdgeData> e) throws Exception {
      Object edgeGroupingValue = e.getValue().getProperty(edgeGroupingKey);
      return new Tuple4<>(e.getValue().getId(), e.getSource(), e.getTarget(),
        (edgeGroupingValue != null) ? edgeGroupingValue.toString() :
          NULL_VALUE);
    }
  }
}
