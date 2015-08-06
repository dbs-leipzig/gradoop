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

package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.io.json.JsonReader;
import org.gradoop.io.json.JsonWriter;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.store.EPGraphStore;

import java.util.Collection;

public class FlinkGraphStore implements EPGraphStore {

  private static GraphData DATABASE_DATA;

  private static Subgraph<Long, GraphData> DATABASE_SUBGRAPH;

  static {
    DATABASE_DATA = GraphDataFactory
      .createDefaultGraphWithID(FlinkConstants.DATABASE_GRAPH_ID);
    DATABASE_SUBGRAPH =
      new Subgraph<>(FlinkConstants.DATABASE_GRAPH_ID, DATABASE_DATA);
  }

  /**
   * Database graph representing the vertex and edge space.
   */
  private EPGraphCollection database;

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment env;

  private FlinkGraphStore(DataSet<Vertex<Long, VertexData>> vertices,
    DataSet<Edge<Long, EdgeData>> edges,
    DataSet<Subgraph<Long, GraphData>> graphData, ExecutionEnvironment env) {
    this.database =
      new EPGraphCollection(Graph.fromDataSet(vertices, edges, env), graphData,
        env);
    this.env = env;
  }

  public static EPGraphStore fromJsonFile(String vertexFile, String edgeFile,
    ExecutionEnvironment env) {
    return fromJsonFile(vertexFile, edgeFile, null, env);
  }

  public static EPGraphStore fromJsonFile(String vertexFile, String edgeFile,
    String graphFile, ExecutionEnvironment env) {
    DataSet<Vertex<Long, VertexData>> vertices =
      env.readTextFile(vertexFile).map(new JsonReader.JsonToVertexMapper());
    DataSet<Edge<Long, EdgeData>> edges =
      env.readTextFile(edgeFile).map(new JsonReader.JsonToEdgeMapper());
    DataSet<Subgraph<Long, GraphData>> graphs;
    if (graphFile != null) {
      graphs =
        env.readTextFile(graphFile).map(new JsonReader.JsonToGraphMapper());
    } else {
      graphs = env.fromCollection(Lists.newArrayList(DATABASE_SUBGRAPH));
    }
    return new FlinkGraphStore(vertices, edges, graphs, env);
  }

  @Override
  public void writeAsJson(final String vertexFile, final String edgeFile,
    final String graphFile) throws Exception {
    getDatabaseGraph().getGellyGraph().getVertices()
      .writeAsFormattedText(vertexFile, new JsonWriter.VertexTextFormatter())
      .getDataSet().collect();
    getDatabaseGraph().getGellyGraph().getEdges()
      .writeAsFormattedText(edgeFile, new JsonWriter.EdgeTextFormatter())
      .getDataSet().collect();
    getCollection().getSubgraphs()
      .writeAsFormattedText(graphFile, new JsonWriter.GraphTextFormatter())
      .getDataSet().collect();
  }

  public static EPGraphStore fromCollection(Collection<VertexData> vertexData,
    Collection<EdgeData> edgeData, ExecutionEnvironment env) {
    return fromCollection(vertexData, edgeData, null, env);
  }

  public static EPGraphStore fromCollection(Collection<VertexData> vertexData,
    Collection<EdgeData> edgeData, Collection<GraphData> graphData,
    ExecutionEnvironment env) {
    DataSet<VertexData> epgmVertexSet = env.fromCollection(vertexData);
    DataSet<EdgeData> epgmEdgeSet = env.fromCollection(edgeData);
    DataSet<GraphData> epgmGraphSet;
    if (graphData != null) {
      epgmGraphSet = env.fromCollection(graphData);
    } else {
      epgmGraphSet = env.fromCollection(Lists.newArrayList(DATABASE_DATA));
    }

    DataSet<Vertex<Long, VertexData>> vertexDataSet = null;
    DataSet<Edge<Long, EdgeData>> edgeDataSet = null;
    DataSet<Subgraph<Long, GraphData>> graphDataSet = null;

    if (epgmVertexSet != null) {
      vertexDataSet = epgmVertexSet.map(new VerticesConverter());
      edgeDataSet = epgmEdgeSet.map(new EdgesConverter());
      graphDataSet = epgmGraphSet.map(new GraphsConverter());
    }
    return new FlinkGraphStore(vertexDataSet, edgeDataSet, graphDataSet, env);
  }

  @Override
  public EPGraph getDatabaseGraph() {
    return EPGraph
      .fromGraph(database.getGraph().getGellyGraph(), DATABASE_DATA);
  }

  @Override
  public EPGraphCollection getCollection() {
    return database;
  }

  @Override
  public EPGraph getGraph(Long graphID) throws Exception {
    return database.getGraph(graphID);
  }

  /**
   * Takes an EPGM vertex and converts it into a flink vertex.
   */
  public static class VerticesConverter implements
    MapFunction<VertexData, Vertex<Long, VertexData>> {

    @Override
    public Vertex<Long, VertexData> map(VertexData value) throws Exception {
      return new Vertex<>(value.getId(), value);
    }
  }

  /**
   * Takes an EPGM vertex and produces a collection of flink edges based on
   * its outgoing edges.
   */
  public static class EdgesConverter implements
    MapFunction<EdgeData, Edge<Long, EdgeData>> {

    @Override
    public Edge<Long, EdgeData> map(EdgeData value) throws Exception {
      return new Edge<>(value.getSourceVertexId(), value.getTargetVertexId(),
        value);
    }
  }

  /**
   * Takes an EPGM vertex and produces a collection of flink edges based on
   * its outgoing edges.
   */
  public static class GraphsConverter implements
    MapFunction<GraphData, Subgraph<Long, GraphData>> {

    @Override
    public Subgraph<Long, GraphData> map(GraphData value) throws Exception {
      return new Subgraph<>(value.getId(), value);
    }
  }
}
