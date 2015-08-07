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
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.io.json.JsonReader;
import org.gradoop.io.json.JsonWriter;
import org.gradoop.model.EdgeData;
import org.gradoop.model.EdgeDataFactory;
import org.gradoop.model.GraphData;
import org.gradoop.model.GraphDataFactory;
import org.gradoop.model.VertexData;
import org.gradoop.model.VertexDataFactory;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.store.EPGraphStore;
import sun.jvmstat.perfdata.monitor.v1_0.BasicType;

import java.util.Collection;

public class FlinkGraphStore<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> implements
  EPGraphStore<VD, ED, GD> {

  private final VertexDataFactory<VD> vertexDataFactory;
  private final EdgeDataFactory<ED> edgeDataFactory;
  private final GraphDataFactory<GD> graphDataFactory;

  private GD databaseData;

  private Subgraph<Long, GD> databaseSubgraph;

  /**
   * Database graph representing the vertex and edge space.
   */
  private EPGraphCollection<VD, ED, GD> database;

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment env;

  private FlinkGraphStore(DataSet<Vertex<Long, VD>> vertices,
    DataSet<Edge<Long, ED>> edges, DataSet<Subgraph<Long, GD>> graphs,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    this.vertexDataFactory = vertexDataFactory;
    this.edgeDataFactory = edgeDataFactory;
    this.graphDataFactory = graphDataFactory;
    this.database =
      new EPGraphCollection<>(Graph.fromDataSet(vertices, edges, env), graphs,
        vertexDataFactory, edgeDataFactory, graphDataFactory, env);
    this.env = env;
    this.databaseData =
      graphDataFactory.createGraphData(FlinkConstants.DATABASE_GRAPH_ID);
    this.databaseSubgraph =
      new Subgraph<>(FlinkConstants.DATABASE_GRAPH_ID, databaseData);
  }

  public static EPGraphStore<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> fromJsonFile(
    String vertexFile, String edgeFile, ExecutionEnvironment env) {
    return fromJsonFile(vertexFile, edgeFile, new DefaultVertexDataFactory(),
      new DefaultEdgeDataFactory(), new DefaultGraphDataFactory(), env);
  }

  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGraphStore<VD, ED, GD> fromJsonFile(
    String vertexFile, String edgeFile, VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    return fromJsonFile(vertexFile, edgeFile, null, vertexDataFactory,
      edgeDataFactory, graphDataFactory, env);
  }

  public static EPGraphStore<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> fromJsonFile(
    String vertexFile, String edgeFile, String graphFile,
    ExecutionEnvironment env) {
    return fromJsonFile(vertexFile, edgeFile, graphFile,
      new DefaultVertexDataFactory(), new DefaultEdgeDataFactory(),
      new DefaultGraphDataFactory(), env);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGraphStore<VD, ED, GD> fromJsonFile(
    String vertexFile, String edgeFile, String graphFile,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {

    TypeInformation<Vertex<Long, VD>> vertexTypeInfo =
      new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(vertexDataFactory.getType()));

    TypeInformation<Edge<Long, ED>> edgeTypeInfo =
      new TupleTypeInfo(Edge.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(edgeDataFactory.getType()));

    TypeInformation<Subgraph<Long, GD>> graphTypeInfo =
      new TupleTypeInfo(Subgraph.class, BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(graphDataFactory.getType()));

    DataSet<Vertex<Long, VD>> vertices = env.readTextFile(vertexFile)
      .map(new JsonReader.JsonToVertexMapper<>(vertexDataFactory))
      .returns(vertexTypeInfo);
    DataSet<Edge<Long, ED>> edges = env.readTextFile(edgeFile)
      .map(new JsonReader.JsonToEdgeMapper<>(edgeDataFactory))
      .returns(edgeTypeInfo);
    DataSet<Subgraph<Long, GD>> graphs;
    if (graphFile != null) {
      graphs = env.readTextFile(graphFile)
        .map(new JsonReader.JsonToGraphMapper<>(graphDataFactory))
        .returns(graphTypeInfo);
    } else {
      graphs = env.fromCollection(Lists.newArrayList(
        new Subgraph<>(FlinkConstants.DATABASE_GRAPH_ID,
          graphDataFactory.createGraphData(FlinkConstants.DATABASE_GRAPH_ID))));
    }

    return new FlinkGraphStore<>(vertices, edges, graphs, vertexDataFactory,
      edgeDataFactory, graphDataFactory, env);
  }

  @Override
  public void writeAsJson(final String vertexFile, final String edgeFile,
    final String graphFile) throws Exception {
    getDatabaseGraph().getGellyGraph().getVertices()
      .writeAsFormattedText(vertexFile,
        new JsonWriter.VertexTextFormatter<VD>()).getDataSet().collect();
    getDatabaseGraph().getGellyGraph().getEdges()
      .writeAsFormattedText(edgeFile, new JsonWriter.EdgeTextFormatter<ED>())
      .getDataSet().collect();
    getCollection().getSubgraphs()
      .writeAsFormattedText(graphFile, new JsonWriter.GraphTextFormatter<GD>())
      .getDataSet().collect();
  }

  public static EPGraphStore<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> fromCollection(
    Collection<DefaultVertexData> vertexDataCollection,
    Collection<DefaultEdgeData> edgeDataCollection, ExecutionEnvironment env) {
    return fromCollection(vertexDataCollection, edgeDataCollection, null,
      new DefaultVertexDataFactory(), new DefaultEdgeDataFactory(),
      new DefaultGraphDataFactory(), env);
  }

  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGraphStore<VD, ED, GD> fromCollection(
    Collection<VD> vertexDataCollection, Collection<ED> edgeDataCollection,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    return fromCollection(vertexDataCollection, edgeDataCollection, null,
      vertexDataFactory, edgeDataFactory, graphDataFactory, env);
  }

  public static EPGraphStore<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> fromCollection(
    Collection<DefaultVertexData> vertexDataCollection,
    Collection<DefaultEdgeData> edgeDataCollection,
    Collection<DefaultGraphData> graphDataCollection,
    ExecutionEnvironment env) {
    return fromCollection(vertexDataCollection, edgeDataCollection,
      graphDataCollection, new DefaultVertexDataFactory(),
      new DefaultEdgeDataFactory(), new DefaultGraphDataFactory(), env);
  }

  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGraphStore<VD, ED, GD> fromCollection(
    Collection<VD> vertexDataCollection, Collection<ED> edgeDataCollection,
    Collection<GD> graphDataCollection, VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    DataSet<VD> epgmVertexSet = env.fromCollection(vertexDataCollection);
    DataSet<ED> epgmEdgeSet = env.fromCollection(edgeDataCollection);
    DataSet<GD> epgmGraphSet;
    if (graphDataCollection != null) {
      epgmGraphSet = env.fromCollection(graphDataCollection);
    } else {
      epgmGraphSet = env.fromCollection(Lists.newArrayList(
        graphDataFactory.createGraphData(FlinkConstants.DATABASE_GRAPH_ID)));
    }

    DataSet<Vertex<Long, VD>> vertexDataSet = null;
    DataSet<Edge<Long, ED>> edgeDataSet = null;
    DataSet<Subgraph<Long, GD>> graphDataSet = null;

    if (epgmVertexSet != null) {
      vertexDataSet = epgmVertexSet.map(new VerticesConverter<VD>());
      edgeDataSet = epgmEdgeSet.map(new EdgesConverter<ED>());
      graphDataSet = epgmGraphSet.map(new GraphsConverter<GD>());
    }
    return new FlinkGraphStore<>(vertexDataSet, edgeDataSet, graphDataSet,
      vertexDataFactory, edgeDataFactory, graphDataFactory, env);
  }

  @Override
  public EPGraph<VD, ED, GD> getDatabaseGraph() {
    return EPGraph.fromGraph(database.getGraph().getGellyGraph(), databaseData,
      vertexDataFactory, edgeDataFactory, graphDataFactory);
  }

  @Override
  public EPGraphCollection<VD, ED, GD> getCollection() {
    return database;
  }

  @Override
  public EPGraph<VD, ED, GD> getGraph(Long graphID) throws Exception {
    return database.getGraph(graphID);
  }

  /**
   * Takes an EPGM vertex and converts it into a flink vertex.
   */
  public static class VerticesConverter<VD extends VertexData> implements
    MapFunction<VD, Vertex<Long, VD>> {

    @Override
    public Vertex<Long, VD> map(VD value) throws Exception {
      return new Vertex<>(value.getId(), value);
    }
  }

  /**
   * Takes an EPGM vertex and produces a collection of flink edges based on
   * its outgoing edges.
   */
  public static class EdgesConverter<ED extends EdgeData> implements
    MapFunction<ED, Edge<Long, ED>> {

    @Override
    public Edge<Long, ED> map(ED value) throws Exception {
      return new Edge<>(value.getSourceVertexId(), value.getTargetVertexId(),
        value);
    }
  }

  /**
   * Takes an EPGM vertex and produces a collection of flink edges based on
   * its outgoing edges.
   */
  public static class GraphsConverter<GD extends GraphData> implements
    MapFunction<GD, Subgraph<Long, GD>> {

    @Override
    public Subgraph<Long, GD> map(GD value) throws Exception {
      return new Subgraph<>(value.getId(), value);
    }
  }
}
