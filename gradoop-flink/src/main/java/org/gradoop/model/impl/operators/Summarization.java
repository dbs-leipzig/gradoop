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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.helper.MathHelper;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkGraphData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

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
public class Summarization implements UnaryGraphToGraphOperator {

  /**
   * Used to represent vertices that do not have the vertex grouping property.
   */
  public static final String NULL_VALUE = "__NULL";

  private final String vertexGroupingKey;

  private final String edgeGroupingKey;

  private final boolean useVertexLabels;

  private final boolean useEdgeLabels;

  private Summarization(String vertexGroupingKey, String edgeGroupingKey,
    boolean useVertexLabels, boolean useEdgeLabels) {
    this.vertexGroupingKey = vertexGroupingKey;
    this.edgeGroupingKey = edgeGroupingKey;
    this.useVertexLabels = useVertexLabels;
    this.useEdgeLabels = useEdgeLabels;
  }

  @Override
  public EPGraph execute(EPGraph graph) {
    EPGraph result;
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> gellyGraph;

    if (vertexGroupingKey != null && !"".equals(vertexGroupingKey)) {
      EPFlinkGraphData graphData = createNewGraphData();
      gellyGraph = summarizeOnVertexProperty(graph.getGellyGraph());
      result = EPGraph.fromGraph(gellyGraph, graphData);
    } else {
      // graphs stays unchanged
      result = graph;
    }
    return result;
  }

  @Override
  public String getName() {
    return "Summarization";
  }

  private EPFlinkGraphData createNewGraphData() {
    EPFlinkGraphData newGraphData = new EPFlinkGraphData();
    newGraphData.setId(FlinkConstants.SUMMARIZE_GRAPH_ID);
    newGraphData.setLabel(FlinkConstants.DEFAULT_GRAPH_LABEL);
    return newGraphData;
  }

  private Graph<Long, EPFlinkVertexData, EPFlinkEdgeData>
  summarizeOnVertexProperty(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph) {
    /* build summarized vertices */

    SortedGrouping<Vertex<Long, EPFlinkVertexData>> groupedSortedVertices =
      graph.getVertices()
        // group vertices by the given property
        .groupBy(new VertexGroupingKeySelector(vertexGroupingKey))
          // sort the group (smallest id is group representative)
        .sortGroup(VERTEX_ID, Order.ASCENDING);

    // create new summarized gelly vertices
    DataSet<Vertex<Long, EPFlinkVertexData>> newVertices = groupedSortedVertices
      .reduceGroup(new VertexGroupSummarizer(vertexGroupingKey));

    // create a list of vertex ids from each vertex group
    DataSet<List<Long>> vertexGroups =
      groupedSortedVertices.reduceGroup(new VertexGroupToList());

    /* build edges */

    // map edges to relevant information (source vertex, target vertex)
    DataSet<Tuple2<Long, Long>> edges =
      graph.getEdges().map(new EdgeProjection());

    // compute cross between vertex groups and edges
    DataSet<Tuple2<List<Long>, Tuple2<Long, Long>>> groupEdgeCross =
      vertexGroups.cross(edges);

    // create intra edges and (possible incomplete) inter edges
    DataSet<Tuple2<Long, Long>> firstRoundEdges =
      groupEdgeCross.flatMap(new FirstRoundFlatMap());

    /* build intra edges */

    DataSet<Edge<Long, EPFlinkEdgeData>> intraEdges = firstRoundEdges
      // filter intra edges (source == target)
      .filter(new IntraEdgeFilter())
        // group them by source vertex
      .groupBy(0)
        // and create new gelly edges with payload
      .reduceGroup(new IntraEdgeGroupSummarizer());

    /* build inter edges */

    DataSet<Tuple2<Long, Long>> secondRoundEdges = firstRoundEdges
      // filter inter edge candidates (source != target)
      .filter(new InterEdgeFilter());

    // cross inter-edges candidates with vertex groups
    groupEdgeCross = vertexGroups.cross(secondRoundEdges);

    // replace target vertex with group representative if possible
    DataSet<Edge<Long, EPFlinkEdgeData>> interEdges = groupEdgeCross
      // finalize inter-edges
      .flatMap(new SecondRoundFlatMap())
        // group them by source and target vertex
      .groupBy(0, 1)
        // and create new gelly edges with payload
      .reduceGroup(new InterEdgeGroupSummarizer());

    /* build final edge set */

    DataSet<Edge<Long, EPFlinkEdgeData>> newEdges =
      interEdges.union(intraEdges);

    return Graph.fromDataSet(newVertices, newEdges, graph.getContext());
  }

  /**
   * Selects the key to group vertices.
   */
  private static class VertexGroupingKeySelector implements
    KeySelector<Vertex<Long, EPFlinkVertexData>, String> {

    private String groupPropertyKey;

    public VertexGroupingKeySelector(String groupPropertyKey) {
      this.groupPropertyKey = groupPropertyKey;
    }

    @Override
    public String getKey(Vertex<Long, EPFlinkVertexData> v) throws Exception {
      if (v.getValue().getProperty(groupPropertyKey) != null) {
        return v.getValue().getProperty(groupPropertyKey).toString();
      } else {
        return NULL_VALUE;
      }
    }
  }

  /**
   * Creates a summarized vertex from a group of vertices.
   */
  private static class VertexGroupSummarizer implements
    GroupReduceFunction<Vertex<Long, EPFlinkVertexData>, Vertex<Long,
      EPFlinkVertexData>> {

    private static final String COUNT_PROPERTY_KEY = "count";

    private String groupPropertyKey;

    public VertexGroupSummarizer(String groupPropertyKey) {
      this.groupPropertyKey = groupPropertyKey;
    }

    @Override
    public void reduce(Iterable<Vertex<Long, EPFlinkVertexData>> iterable,
      Collector<Vertex<Long, EPFlinkVertexData>> collector) throws Exception {
      int groupCount = 0;
      Long newVertexID = 0L;
      String groupValue = null;
      boolean initialized = false;
      for (Vertex<Long, EPFlinkVertexData> v : iterable) {
        groupCount++;
        if (!initialized) {
          // will be the minimum vertex id in the group
          newVertexID = v.getId();
          if (v.getValue().getProperty(groupPropertyKey) != null) {
            groupValue = v.getValue().getProperty(groupPropertyKey).toString();
          } else {
            groupValue = NULL_VALUE;
          }
          initialized = true;
        }
      }

      EPFlinkVertexData newVertexData = new EPFlinkVertexData();
      newVertexData.setId(newVertexID);
      newVertexData.setLabel(FlinkConstants.DEFAULT_VERTEX_LABEL);
      newVertexData.setProperty(groupPropertyKey, groupValue);
      newVertexData.setProperty(COUNT_PROPERTY_KEY, groupCount);
      newVertexData.addGraph(FlinkConstants.SUMMARIZE_GRAPH_ID);

      collector.collect(new Vertex<>(newVertexID, newVertexData));
    }
  }

  /**
   * Creates a summarized intra-edge from a group of edges.
   */
  private static class IntraEdgeGroupSummarizer implements
    GroupReduceFunction<Tuple2<Long, Long>, Edge<Long, EPFlinkEdgeData>> {

    @Override
    public void reduce(Iterable<Tuple2<Long, Long>> iterable,
      Collector<Edge<Long, EPFlinkEdgeData>> collector) throws Exception {
      int edgeCount = 0;
      boolean initialized = false;
      Long vertexID = null;

      for (Tuple2<Long, Long> t : iterable) {
        edgeCount++;
        if (!initialized) {
          vertexID = t.f0;
          initialized = true;
        }
      }
      if (vertexID != null) {
        EPFlinkEdgeData newEdgeData = new EPFlinkEdgeData();
        newEdgeData.setId(MathHelper.cantor(vertexID, vertexID));
        newEdgeData.setLabel(FlinkConstants.DEFAULT_EDGE_LABEL);
        newEdgeData.setSourceVertex(vertexID);
        newEdgeData.setTargetVertex(vertexID);
        newEdgeData.setProperty("count", edgeCount);
        newEdgeData.addGraph(FlinkConstants.SUMMARIZE_GRAPH_ID);
        collector.collect(new Edge<>(vertexID, vertexID, newEdgeData));
      }
    }
  }

  /**
   * Creates a summarized inter-edge from a group of edges.
   */
  private static class InterEdgeGroupSummarizer implements
    GroupReduceFunction<Tuple2<Long, Long>, Edge<Long, EPFlinkEdgeData>> {

    @Override
    public void reduce(Iterable<Tuple2<Long, Long>> iterable,
      Collector<Edge<Long, EPFlinkEdgeData>> collector) throws Exception {
      int edgeCount = 0;
      boolean initialized = false;
      Long sourceVertexID = null;
      Long targetVertexID = null;

      for (Tuple2<Long, Long> t : iterable) {
        edgeCount++;
        if (!initialized) {
          sourceVertexID = t.f0;
          targetVertexID = t.f1;
          initialized = true;
        }
      }
      if (sourceVertexID != null && targetVertexID != null) {
        EPFlinkEdgeData newEdgeData = new EPFlinkEdgeData();
        newEdgeData.setId(MathHelper.cantor(sourceVertexID, targetVertexID));
        newEdgeData.setLabel(FlinkConstants.DEFAULT_EDGE_LABEL);
        newEdgeData.setSourceVertex(sourceVertexID);
        newEdgeData.setTargetVertex(targetVertexID);
        newEdgeData.setProperty("count", edgeCount);
        newEdgeData.addGraph(FlinkConstants.SUMMARIZE_GRAPH_ID);
        collector
          .collect(new Edge<>(sourceVertexID, targetVertexID, newEdgeData));
      }
    }
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
    FlatMapFunction<Tuple2<List<Long>, Tuple2<Long, Long>>, Tuple2<Long,
      Long>> {

    @Override
    public void flatMap(Tuple2<List<Long>, Tuple2<Long, Long>> t,
      Collector<Tuple2<Long, Long>> coll) throws Exception {
      List<Long> sortedVertexGroup = t.f0;
      Tuple2<Long, Long> edge = t.f1;
      // list is sorted, representative is the first element
      Long groupRepresentative = sortedVertexGroup.get(0);
      // source vertex in group ?
      if (sortedVertexGroup.contains(edge.f0)) {
        // target in vertex group ?
        if (sortedVertexGroup.contains(edge.f1)) {
          // create an intra edge
          coll.collect(new Tuple2<>(groupRepresentative, groupRepresentative));
        } else {
          // create an inter edge
          coll.collect(new Tuple2<>(groupRepresentative, edge.f1));
        }
      }
    }
  }

  /**
   * Creates final inter edges by replacing the target vertex with the
   * corresponding vertex group representative.
   */
  private static class SecondRoundFlatMap implements
    FlatMapFunction<Tuple2<List<Long>, Tuple2<Long, Long>>, Tuple2<Long,
      Long>> {

    @Override
    public void flatMap(Tuple2<List<Long>, Tuple2<Long, Long>> t,
      Collector<Tuple2<Long, Long>> coll) throws Exception {
      List<Long> sortedVertexGroup = t.f0;
      Tuple2<Long, Long> edge = t.f1;
      // list is sorted, representative is the first element
      Long groupRepresentative = sortedVertexGroup.get(0);

      // target vertex in group?
      if (sortedVertexGroup.contains(edge.f1)) {
        coll.collect(new Tuple2<>(edge.f0, groupRepresentative));
      }
    }
  }

  /**
   * Filters intra edges (source-vertex == target-vertex).
   */
  private static class IntraEdgeFilter implements
    FilterFunction<Tuple2<Long, Long>> {
    @Override
    public boolean filter(Tuple2<Long, Long> t) throws Exception {
      return t.f0.equals(t.f1);
    }
  }

  /**
   * Filters inter edges (source-vertex != target-vertex).
   */
  private static class InterEdgeFilter implements
    FilterFunction<Tuple2<Long, Long>> {

    @Override
    public boolean filter(Tuple2<Long, Long> t) throws Exception {
      return !t.f0.equals(t.f1);
    }
  }

  /**
   * Reduces edges to the information which is relevant for further
   * processing (source vertex and target vertex).
   */
  private static class EdgeProjection implements
    MapFunction<Edge<Long, EPFlinkEdgeData>, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> map(Edge<Long, EPFlinkEdgeData> e) throws
      Exception {
      return new Tuple2<>(e.getSource(), e.getTarget());
    }
  }

  public static class SummarizationBuilder {

    private String vertexGroupingKey;

    private String edgeGroupingKey;

    private boolean useVertexLabels = false;

    private boolean useEdgeLabels = false;

    public SummarizationBuilder vertexGroupingKey(
      final String vertexGroupingKey) {
      this.vertexGroupingKey = vertexGroupingKey;
      return this;
    }

    public SummarizationBuilder edgeGroupingKey(final String edgeGroupingKey) {
      this.edgeGroupingKey = edgeGroupingKey;
      return this;
    }

    public SummarizationBuilder useVertexLabels(final boolean useVertexLabels) {
      this.useVertexLabels = useVertexLabels;
      return this;
    }

    public SummarizationBuilder useEdgeLabels(final boolean useEdgeLabels) {
      this.useEdgeLabels = useEdgeLabels;
      return this;
    }

    public Summarization build() {
      return new Summarization(vertexGroupingKey, edgeGroupingKey,
        useVertexLabels, useEdgeLabels);
    }
  }
}
