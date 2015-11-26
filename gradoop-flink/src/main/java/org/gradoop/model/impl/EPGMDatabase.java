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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.gradoop.io.hbase.HBaseReader;
import org.gradoop.io.hbase.HBaseWriter;
import org.gradoop.io.json.JsonReader;
import org.gradoop.io.json.JsonWriter;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.ImportIdGenerator;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
import org.gradoop.model.impl.tuples.Subgraph;
import org.gradoop.storage.api.EPGMStore;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentEdgeFactory;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentGraphHeadFactory;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;
import org.gradoop.util.FlinkConstants;
import org.gradoop.util.GradoopConfig;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Collection;

/**
 * Enables the access an EPGM instance.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public class EPGMDatabase
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig<V, E, G> config;

  /**
   * Database graph representing the vertex and edge space.
   */
  private GraphCollection<G, V, E> database;

  /**
   * Graph data associated with that logical graph.
   */
  private final G databaseData;

  /**
   * Creates a new EPGM database from the given arguments.
   *
   * @param vertices    vertex data set
   * @param edges       edge data set
   * @param graphHeads  graph data set
   * @param config      Gradoop Flink Configuration
   */
  private EPGMDatabase(DataSet<V> vertices,
    DataSet<E> edges, DataSet<G> graphHeads,
    GradoopFlinkConfig<V, E, G> config) {
    this.config = config;
    this.database = GraphCollection.fromDataSets(vertices,
      edges, graphHeads, config);
    this.databaseData = config.getGraphHeadHandler().getGraphHeadFactory()
      .initGraphHead(FlinkConstants.DATABASE_GRAPH_ID);
  }

  /**
   * Creates a database from JSON files. Paths can be local (file://) or HDFS
   * (hdfs://).
   * <p/>
   * Uses default factories for POJO creation.
   *
   * @param vertexFile path to vertex data file
   * @param edgeFile   path to edge data file
   * @param env        Flink execution environment
   * @return EPGM database with default factories.
   * @see VertexPojoFactory
   * @see EdgePojoFactory
   * @see GraphHeadPojoFactory
   */
  @SuppressWarnings("unchecked")
  public static EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo> fromJsonFile(
    String vertexFile, String edgeFile, ExecutionEnvironment env) {
    return fromJsonFile(vertexFile, edgeFile,
      GradoopFlinkConfig.createDefaultConfig(env));
  }

  /**
   * Creates a database from JSON files. Paths can be local (file://) or HDFS
   * (hdfs://). Data POJOs are created using the given factories.
   *
   * @param vertexFile  path to vertex data file
   * @param edgeFile    path to edge data file
   * @param config      Gradoop Flink configuration
   * @param <VD>        vertex data type
   * @param <ED>        edge data type
   * @param <GD>        graph data type
   * @return EPGM database
   */
  public static
  <VD extends EPGMVertex, ED extends EPGMEdge, GD extends EPGMGraphHead>
  EPGMDatabase fromJsonFile(
    String vertexFile, String edgeFile, GradoopFlinkConfig<VD, ED, GD> config) {
    return fromJsonFile(vertexFile, edgeFile, null, config);
  }

  /**
   * Creates a database from JSON files. Paths can be local (file://) or HDFS
   * (hdfs://).
   * <p/>
   * Uses default factories for POJO creation.
   *
   * @param vertexFile vertex data file
   * @param edgeFile   edge data file
   * @param graphFile  graph data file
   * @param env        Flink execution environment
   * @return EPGM database
   * @see VertexPojoFactory
   * @see EdgePojoFactory
   * @see GraphHeadPojoFactory
   */
  @SuppressWarnings("unchecked")
  public static EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo> fromJsonFile(
    String vertexFile, String edgeFile, String graphFile,
    ExecutionEnvironment env) {
    return fromJsonFile(vertexFile, edgeFile, graphFile,
      GradoopFlinkConfig.createDefaultConfig(env));
  }

  /**
   * Creates a database from JSON files. Paths can be local (file://) or HDFS
   * (hdfs://).
   *
   * @param vertexFile  vertex data file
   * @param edgeFile    edge data file
   * @param graphFile   graph data file
   * @param config      Gradoop Flink configuration
   * @param <VD>        EPGM vertex type
   * @param <ED>        EPGM edge type
   * @param <GD>        EPGM graph head type
   *
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static
  <VD extends EPGMVertex, ED extends EPGMEdge, GD extends EPGMGraphHead>
  EPGMDatabase fromJsonFile(
    String vertexFile, String edgeFile, String graphFile,
    GradoopFlinkConfig<VD, ED, GD> config) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }

    ExecutionEnvironment env = config.getExecutionEnvironment();

    // used for type hinting when loading vertex data
    TypeInformation<VD> vertexTypeInfo = (TypeInformation<VD>)
      TypeExtractor.createTypeInfo(config.getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<ED> edgeTypeInfo = (TypeInformation<ED>)
      TypeExtractor.createTypeInfo(config.getEdgeFactory().getType());
    // used for type hinting when loading graph data
    TypeInformation<GD> graphTypeInfo = (TypeInformation<GD>)
      TypeExtractor.createTypeInfo(config.getGraphHeadFactory().getType());

    // read vertex, edge and graph data
    ImportIdGenerator idGenerator = new ImportIdGenerator();

    DataSet<VD> vertices = env.readTextFile(vertexFile)
      .map(new JsonReader.JsonToVertexMapper<>(
        config.getVertexFactory(), idGenerator))
      .returns(vertexTypeInfo);
    DataSet<ED> edges = env.readTextFile(edgeFile)
      .map(new JsonReader.JsonToEdgeMapper<>(
        config.getEdgeFactory(), idGenerator))
      .returns(edgeTypeInfo);
    DataSet<GD> graphHeads;
    if (graphFile != null) {
      graphHeads = env.readTextFile(graphFile)
        .map(new JsonReader.JsonToGraphMapper<>(
          config.getGraphHeadFactory(), idGenerator))
        .returns(graphTypeInfo);
    } else {
      graphHeads = env.fromCollection(Lists.newArrayList(
        config.getGraphHeadFactory()
          .initGraphHead(FlinkConstants.DATABASE_GRAPH_ID)));
    }

    return new EPGMDatabase<>(vertices, edges, graphHeads, config);
  }

  /**
   * Writes the epgm database into three separate JSON files. {@code
   * vertexFile} contains all vertex data, {@code edgeFile} contains all edge
   * data and {@code graphFile} contains graph data of all logical graphs.
   * <p/>
   * Operation uses Flink to write the internal datasets, thus writing to
   * local file system ({@code file://}) as well as HDFS ({@code hdfs://}) is
   * supported.
   *
   * @param vertexFile vertex data output file
   * @param edgeFile   edge data output file
   * @param graphFile  graph data output file
   * @throws Exception
   */
  public void writeAsJson(final String vertexFile, final String edgeFile,
    final String graphFile) throws Exception {
    getDatabaseGraph().getVertices()
      .writeAsFormattedText(vertexFile,
        new JsonWriter.VertexTextFormatter<V>());
    getDatabaseGraph().getEdges()
      .writeAsFormattedText(edgeFile, new JsonWriter.EdgeTextFormatter<E>());
    getCollection().getGraphHeads()
      .writeAsFormattedText(graphFile, new JsonWriter.GraphTextFormatter<G>());
    config.getExecutionEnvironment().execute();
  }

  /**
   * Writes the EPGM database instance to HBase using the given arguments.
   * <p/>
   * HBase tables must be created before calling this method.
   *
   * @param epgmStore                   EPGM store to handle HBase
   * @param persistentVertexFactory persistent vertex data factory
   * @param persistentEdgeFactory   persistent edge data factory
   * @param persistentGraphHeadFactory  persistent graph data factory
   * @param <PVD>                       persistent vertex data type
   * @param <PED>                       persistent edge data type
   * @param <PGD>                       persistent graph data type
   * @throws Exception
   */
  public <PVD extends PersistentVertex<E>, PED
    extends PersistentEdge<V>, PGD extends PersistentGraphHead>
  void writeToHBase(
    EPGMStore<V, E, G> epgmStore,
    final PersistentVertexFactory<V, E, PVD> persistentVertexFactory,
    final PersistentEdgeFactory<E, V, PED> persistentEdgeFactory,
    final PersistentGraphHeadFactory<G, PGD> persistentGraphHeadFactory) throws
    Exception {

    HBaseWriter<V, E, G> hBaseWriter = new HBaseWriter<>();

    GradoopConfig<G, V, E> conf = epgmStore.getConfig();
    // transform graph data to persistent graph data and write it
    hBaseWriter.writeGraphHeads(this, conf.getGraphHeadHandler(),
      persistentGraphHeadFactory, epgmStore.getGraphHeadName());
    this.config.getExecutionEnvironment().execute();

    // transform vertex data to persistent vertex data and write it
    hBaseWriter.writeVertices(this, conf.getVertexHandler(),
      persistentVertexFactory, epgmStore.getVertexTableName());
    this.config.getExecutionEnvironment().execute();

    // transform edge data to persistent edge data and write it
    hBaseWriter.writeEdges(this, conf.getEdgeHandler(), persistentEdgeFactory,
      epgmStore.getEdgeTableName());
    this.config.getExecutionEnvironment().execute();
  }

  /**
   * Creates a database from collections of vertex and edge data objects.
   * <p/>
   * Uses default factories for POJO creation.
   *
   * @param vertexDataCollection collection of vertex data objects
   * @param edgeDataCollection   collection of edge data objects
   * @param env                  Flink execution environment
   * @return EPGM database
   * @see VertexPojoFactory
   * @see EdgePojoFactory
   * @see GraphHeadPojoFactory
   */
  @SuppressWarnings("unchecked")
  public static EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo>
  fromCollection(
    Collection<VertexPojo> vertexDataCollection,
    Collection<EdgePojo> edgeDataCollection, ExecutionEnvironment env) {
    return fromCollection(vertexDataCollection, edgeDataCollection, null,
      GradoopFlinkConfig.createDefaultConfig(env));
  }

  /**
   * Creates a database from collections of vertex and data objects.
   *
   * @param vertexDataCollection  collection of vertex data objects
   * @param edgeDataCollection    collection of edge data objects
   * @param config                Gradoop Flink Config
   * @param <VD>                  vertex data type
   * @param <ED>                  edge data type
   * @param <GD>                  graph data typeFlink execution environment
   * @return EPGM database
   */
  public static
  <VD extends EPGMVertex, ED extends EPGMEdge, GD extends EPGMGraphHead>
  EPGMDatabase fromCollection(
    Collection<VD> vertexDataCollection, Collection<ED> edgeDataCollection,
    GradoopFlinkConfig<VD, ED, GD> config) {
    return fromCollection(vertexDataCollection, edgeDataCollection, null,
      config);
  }

  /**
   * Creates a database from collections of vertex, edge and graph data
   * objects.
   * <p/>
   * Uses default factories for POJO creation.
   *
   * @param vertexDataCollection collection of vertex data objects
   * @param edgeDataCollection   collection of edge data objects
   * @param graphDataCollection  collection of graph data objects
   * @param env                  Flink execution environment
   * @return EPGM database
   * @see VertexPojoFactory
   * @see EdgePojoFactory
   * @see GraphHeadPojoFactory
   */
  @SuppressWarnings("unchecked")
  public static EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo>
  fromCollection(
    Collection<VertexPojo> vertexDataCollection,
    Collection<EdgePojo> edgeDataCollection,
    Collection<GraphHeadPojo> graphDataCollection,
    ExecutionEnvironment env) {
    return fromCollection(vertexDataCollection, edgeDataCollection,
      graphDataCollection, GradoopFlinkConfig.createDefaultConfig(env));
  }

  /**
   * Creates a database from collections of vertex and data objects.
   *
   * @param vertexDataCollection  collection of vertices
   * @param edgeDataCollection    collection of edges
   * @param graphDataCollection   collection of graph heads
   * @param config                Gradoop Flink configuration
   * @param <VD>                  vertex data type
   * @param <ED>                  edge data type
   * @param <GD>                  graph data type
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static
  <VD extends EPGMVertex, ED extends EPGMEdge, GD extends EPGMGraphHead>
  EPGMDatabase fromCollection(

    Collection<VD> vertexDataCollection,
    Collection<ED> edgeDataCollection,
    Collection<GD> graphDataCollection,
    GradoopFlinkConfig<VD, ED, GD> config) {
    if (config == null) {
      throw new IllegalArgumentException("Config must not be null");
    }
    ExecutionEnvironment env = config.getExecutionEnvironment();
    DataSet<VD> vertices = env.fromCollection(vertexDataCollection);
    DataSet<ED> edges = env.fromCollection(edgeDataCollection);
    DataSet<GD> graphHeads;
    if (graphDataCollection != null) {
      graphHeads = env.fromCollection(graphDataCollection);
    } else {
      graphHeads = env.fromCollection(Lists.newArrayList(
        config.getGraphHeadFactory()
          .initGraphHead(FlinkConstants.DATABASE_GRAPH_ID)));
    }

    return new EPGMDatabase<>(vertices, edges, graphHeads, config);
  }

  /**
   * Creates an EPGM database from an EPGM Store using the given arguments.
   *
   * @param epgmStore EPGM store
   * @param env       Flink execution environment
   * @param <VD>      vertex data type
   * @param <ED>      edge data type
   * @param <GD>      graph data type
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static
  <VD extends EPGMVertex, ED extends EPGMEdge, GD extends EPGMGraphHead>
  EPGMDatabase<GD, VD, ED> fromHBase(
    EPGMStore<VD, ED, GD> epgmStore, ExecutionEnvironment env) {

    GradoopConfig<GD, VD, ED> conf = epgmStore.getConfig();
    // used for type hinting when loading vertex data
    TypeInformation<Vertex<GradoopId, VD>> vertexTypeInfo =
      new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(
          epgmStore.getConfig().getVertexFactory().getType()));
    // used for type hinting when loading edge data
    TypeInformation<Edge<GradoopId, ED>> edgeTypeInfo =
      new TupleTypeInfo(Edge.class, BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(
          conf.getEdgeHandler().getEdgeFactory().getType()));
    // used for type hinting when loading graph data
    TypeInformation<Subgraph<GradoopId, GD>> graphTypeInfo =
      new TupleTypeInfo(Subgraph.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(
          conf.getGraphHeadHandler().getGraphHeadFactory().getType()));

    DataSet<Vertex<GradoopId, VD>> vertexDataSet = env.createInput(
      new HBaseReader.VertexTableInputFormat<>(
        conf.getVertexHandler(), epgmStore.getVertexTableName()),
      vertexTypeInfo);

    DataSet<Edge<GradoopId, ED>> edgeDataSet = env.createInput(
      new HBaseReader.EdgeTableInputFormat<>(conf.getEdgeHandler(),
        epgmStore.getEdgeTableName()), edgeTypeInfo);

    DataSet<Subgraph<GradoopId, GD>> subgraphDataSet = env.createInput(
      new HBaseReader.GraphHeadTableInputFormat<>(
        conf.getGraphHeadHandler(), epgmStore.getGraphHeadName()),
      graphTypeInfo);

    return new EPGMDatabase<>(
      vertexDataSet.map(new MapFunction<Vertex<GradoopId, VD>, VD>() {
        @Override
        public VD map(Vertex<GradoopId, VD> longVDVertex) throws Exception {
          return longVDVertex.getValue();
        }
      }).withForwardedFields("f1->*"),
      edgeDataSet.map(new MapFunction<Edge<GradoopId, ED>, ED>() {
        @Override
        public ED map(Edge<GradoopId, ED> longEDEdge) throws Exception {
          return longEDEdge.getValue();
        }
      }).withForwardedFields("f2->*"),
      subgraphDataSet.map(new MapFunction<Subgraph<GradoopId, GD>, GD>() {
        @Override
        public GD map(Subgraph<GradoopId, GD> longGDSubgraph) throws Exception {
          return longGDSubgraph.getValue();
        }
      }).withForwardedFields("f1->*"),
      GradoopFlinkConfig.createConfig(conf, env));
  }

  /**
   * Returns a logical graph containing the complete vertex and edge space of
   * that EPGM database.
   *
   * @return logical graph of vertex and edge space
   */
  public LogicalGraph<G, V, E> getDatabaseGraph() {
    return LogicalGraph
      .fromDataSets(database.getVertices(), database.getEdges(), config);
  }

  /**
   * Returns a logical graph by its identifier.
   *
   * @param graphID graph identifier
   * @return logical graph or {@code null} if graph does not exist
   * @throws Exception
   */
  public LogicalGraph<G, V, E> getGraph(GradoopId graphID) throws Exception {
    return database.getGraph(graphID);
  }

  /**
   * Returns a collection of all logical graph contained in that EPGM database.
   *
   * @return collection of all logical graphs
   */
  public GraphCollection<G, V, E> getCollection() {
    DataSet<V> newVertices =
      database.getVertices()
        .filter(new FilterFunction<V>() {
          @Override
          public boolean filter(V vertex) throws
            Exception {
            return vertex.getGraphCount() > 0;
          }
        });
    DataSet<E> newEdges = database.getEdges()
      .filter(new FilterFunction<E>() {
        @Override
        public boolean filter(E longEDEdge) throws Exception {
          return longEDEdge.getGraphCount() > 0;
        }
      });

    return GraphCollection.fromDataSets(newVertices,
      newEdges, database.getGraphHeads(), config);
  }
}
