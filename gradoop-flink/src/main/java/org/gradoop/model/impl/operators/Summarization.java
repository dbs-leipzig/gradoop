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
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkGraphData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

import static org.gradoop.model.impl.EPGraph.VERTEX_ID;

public abstract class Summarization implements UnaryGraphToGraphOperator {
  /**
   * Used to represent vertices that do not have the vertex grouping property.
   */
  public static final String NULL_VALUE = "__NULL";

  protected final String vertexGroupingKey;

  protected final String edgeGroupingKey;

  protected final boolean useVertexLabels;

  protected final boolean useEdgeLabels;

  Summarization(String vertexGroupingKey, String edgeGroupingKey,
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
      gellyGraph = summarizeInternal(graph.getGellyGraph());
      result = EPGraph.fromGraph(gellyGraph, graphData);
    } else {
      // graphs stays unchanged
      result = graph;
    }
    return result;
  }

  protected SortedGrouping<Vertex<Long, EPFlinkVertexData>>
  groupAndSortVertices(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph) {
    return graph.getVertices()
      // group vertices by the given property
      .groupBy(new VertexGroupingKeySelector(vertexGroupingKey))
        // sort the group (smallest id is group representative)
      .sortGroup(VERTEX_ID, Order.ASCENDING);
  }

  protected DataSet<Vertex<Long, EPFlinkVertexData>> buildSummarizedVertices(
    SortedGrouping<Vertex<Long, EPFlinkVertexData>> groupedSortedVertices) {
    return groupedSortedVertices
      .reduceGroup(new VertexGroupSummarizer(vertexGroupingKey));
  }


  private EPFlinkGraphData createNewGraphData() {
    EPFlinkGraphData newGraphData = new EPFlinkGraphData();
    newGraphData.setId(FlinkConstants.SUMMARIZE_GRAPH_ID);
    newGraphData.setLabel(FlinkConstants.DEFAULT_GRAPH_LABEL);
    return newGraphData;
  }

  protected abstract Graph<Long, EPFlinkVertexData, EPFlinkEdgeData>
  summarizeInternal(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph);

  /**
   * Selects the key to group vertices.
   */
  protected static class VertexGroupingKeySelector implements
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
  protected static class VertexGroupSummarizer implements
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
   * Creates a summarized edge from a group of edges.
   */
  protected static class EdgeGroupSummarizer implements
    GroupReduceFunction<Tuple3<Long, Long, Long>, Edge<Long, EPFlinkEdgeData>> {

    @Override
    public void reduce(Iterable<Tuple3<Long, Long, Long>> iterable,
      Collector<Edge<Long, EPFlinkEdgeData>> collector) throws Exception {
      int edgeCount = 0;
      boolean initialized = false;
      // new edge id will be the first edge id in the group (which is sorted)
      Long newEdgeID = null;
      Long newSourceVertex = null;
      Long newTargetVertex = null;

      for (Tuple3<Long, Long, Long> t : iterable) {
        edgeCount++;
        if (!initialized) {
          newEdgeID = t.f0;
          newSourceVertex = t.f1;
          newTargetVertex = t.f2;
          initialized = true;
        }
      }
      EPFlinkEdgeData newEdgeData = new EPFlinkEdgeData();
      newEdgeData.setId(newEdgeID);
      newEdgeData.setLabel(FlinkConstants.DEFAULT_EDGE_LABEL);
      newEdgeData.setSourceVertex(newSourceVertex);
      newEdgeData.setTargetVertex(newTargetVertex);
      newEdgeData.setProperty("count", edgeCount);
      newEdgeData.addGraph(FlinkConstants.SUMMARIZE_GRAPH_ID);
      collector
        .collect(new Edge<>(newSourceVertex, newTargetVertex, newEdgeData));
    }
  }

  /**
   * Creates a summarized edge from a group of edges including a edge
   * grouping value.
   */
  protected static class EdgeGroupSummarizerWithGroupValue implements
    GroupReduceFunction<Tuple4<Long, Long, Long, String>, Edge<Long,
      EPFlinkEdgeData>> {

    private String edgeGroupingKey;

    public EdgeGroupSummarizerWithGroupValue(String edgeGroupingKey) {
      this.edgeGroupingKey = edgeGroupingKey;
    }

    @Override
    public void reduce(Iterable<Tuple4<Long, Long, Long, String>> iterable,
      Collector<Edge<Long, EPFlinkEdgeData>> collector) throws Exception {
      int edgeCount = 0;
      boolean initialized = false;
      // new edge id will be the first edge id in the group (which is sorted)
      Long newEdgeID = null;
      Long newSourceVertex = null;
      Long newTargetVertex = null;
      String edgeGroupingValue = null;

      for (Tuple4<Long, Long, Long, String> t : iterable) {
        edgeCount++;
        if (!initialized) {
          newEdgeID = t.f0;
          newSourceVertex = t.f1;
          newTargetVertex = t.f2;
          edgeGroupingValue = t.f3;
          initialized = true;
        }
      }
      EPFlinkEdgeData newEdgeData = new EPFlinkEdgeData();
      newEdgeData.setId(newEdgeID);
      newEdgeData.setLabel(FlinkConstants.DEFAULT_EDGE_LABEL);
      newEdgeData.setSourceVertex(newSourceVertex);
      newEdgeData.setTargetVertex(newTargetVertex);
      newEdgeData.setProperty(edgeGroupingKey, edgeGroupingValue);
      newEdgeData.setProperty("count", edgeCount);
      newEdgeData.addGraph(FlinkConstants.SUMMARIZE_GRAPH_ID);
      collector
        .collect(new Edge<>(newSourceVertex, newTargetVertex, newEdgeData));
    }
  }

  public static class SummarizationBuilder {

    private String vertexGroupingKey;

    private String edgeGroupingKey;

    private boolean useVertexLabels = false;

    private boolean useEdgeLabels = false;

    private boolean useJoinOp = false;

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

    public SummarizationBuilder setUseJoinOp(boolean useJoinOp) {
      this.useJoinOp = useJoinOp;
      return this;
    }

    public Summarization build() {
      if (useJoinOp) {
        return new SummarizationJoin(vertexGroupingKey, edgeGroupingKey,
          useVertexLabels, useEdgeLabels);
      } else {
        return new SummarizationCross(vertexGroupingKey, edgeGroupingKey,
          useVertexLabels, useEdgeLabels);
      }
    }
  }
}
