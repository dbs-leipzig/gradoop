/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
import org.gradoop.model.impl.tuples.Subgraph;
import org.gradoop.storage.api.EPGMStore;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentEdgeFactory;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentGraphHeadFactory;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;
import org.gradoop.util.GConstants;
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
public class EPGMDatabase<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge> {

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * Database graph representing the vertex and edge space.
   */
  private GraphCollection<G, V, E> database;

  /**
   * Graph head representing the database graph.
   */
  private final DataSet<G> graphHead;

  /**
   * Creates a new EPGM database from the given arguments.
   *
   * @param vertices    vertex data set
   * @param edges       edge data set
   * @param graphHeads  graph data set
   * @param config      Gradoop Flink Configuration
   */
  private EPGMDatabase(DataSet<G> graphHeads,
    DataSet<V> vertices,
    DataSet<E> edges,
    GradoopFlinkConfig<G, V, E> config) {
    this.config = config;
    this.database = GraphCollection.fromDataSets(graphHeads, vertices,
      edges, config);
    graphHead = config.getExecutionEnvironment().fromElements(
      config.getGraphHeadFactory().createGraphHead(GConstants.DB_GRAPH_LABEL));
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
   * @param <G>         EPGM grpah head type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @return EPGM database
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> EPGMDatabase fromJsonFile(
    String vertexFile, String edgeFile, GradoopFlinkConfig<G, V, E> config) {
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
  public static EPGMDatabase<
    GraphHeadPojo,
    VertexPojo,
    EdgePojo> fromJsonFile(
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
   * @param <V>        EPGM vertex type
   * @param <E>        EPGM edge type
   * @param <G>        EPGM graph head type
   *
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge>
  EPGMDatabase fromJsonFile(
    String vertexFile, String edgeFile, String graphFile,
    GradoopFlinkConfig<G, V, E> config) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }

    ExecutionEnvironment env = config.getExecutionEnvironment();

    // used for type hinting when loading vertex data
    TypeInformation<V> vertexTypeInfo = (TypeInformation<V>)
      TypeExtractor.createTypeInfo(config.getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<E> edgeTypeInfo = (TypeInformation<E>)
      TypeExtractor.createTypeInfo(config.getEdgeFactory().getType());
    // used for type hinting when loading graph data
    TypeInformation<G> graphTypeInfo = (TypeInformation<G>)
      TypeExtractor.createTypeInfo(config.getGraphHeadFactory().getType());

    // read vertex, edge and graph data

    DataSet<V> vertices = env.readTextFile(vertexFile)
      .map(new JsonReader.JsonToVertexMapper<>(config.getVertexFactory()))
      .returns(vertexTypeInfo);
    DataSet<E> edges = env.readTextFile(edgeFile)
      .map(new JsonReader.JsonToEdgeMapper<>(config.getEdgeFactory()))
      .returns(edgeTypeInfo);
    DataSet<G> graphHeads;
    if (graphFile != null) {
      graphHeads = env.readTextFile(graphFile)
        .map(new JsonReader.JsonToGraphMapper<>(config.getGraphHeadFactory()))
        .returns(graphTypeInfo);
    } else {
      graphHeads = env.fromElements(
        config.getGraphHeadFactory().createGraphHead());
    }

    return new EPGMDatabase<>(graphHeads, vertices, edges, config);
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
   * @param persistentVertexFactory     persistent vertex data factory
   * @param persistentEdgeFactory       persistent edge data factory
   * @param persistentGraphHeadFactory  persistent graph data factory
   * @param <PG>                        persistent graph head type
   * @param <PV>                        persistent vertex type
   * @param <PE>                        persistent edge type
   *
   * @throws Exception
   */
  public <
    PG extends PersistentGraphHead,
    PV extends PersistentVertex<E>,
    PE extends PersistentEdge<V>> void writeToHBase(
    EPGMStore<G, V, E> epgmStore,
    final PersistentVertexFactory<V, E, PV> persistentVertexFactory,
    final PersistentEdgeFactory<V, E, PE> persistentEdgeFactory,
    final PersistentGraphHeadFactory<G, PG> persistentGraphHeadFactory) throws
    Exception {

    HBaseWriter<G, V, E> hBaseWriter = new HBaseWriter<>();

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
    return fromCollection(null, vertexDataCollection, edgeDataCollection,
      GradoopFlinkConfig.createDefaultConfig(env));
  }

  /**
   * Creates a database from collections of vertex and data objects.
   *
   * @param vertexDataCollection  collection of vertex data objects
   * @param edgeDataCollection    collection of edge data objects
   * @param config                Gradoop Flink Config
   * @param <G>                   EPGM graph head type
   * @param <V>                   EPGM vertex type
   * @param <E>                   EPGM edge type
   * @return EPGM database
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> EPGMDatabase fromCollection(
    Collection<V> vertexDataCollection, Collection<E> edgeDataCollection,
    GradoopFlinkConfig<G, V, E> config) {
    return fromCollection(null, vertexDataCollection, edgeDataCollection,
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
    Collection<GraphHeadPojo> graphDataCollection,
    Collection<VertexPojo> vertexDataCollection,
    Collection<EdgePojo> edgeDataCollection,
    ExecutionEnvironment env) {
    return fromCollection(graphDataCollection, vertexDataCollection,
      edgeDataCollection, GradoopFlinkConfig.createDefaultConfig(env));
  }

  /**
   * Creates a database from collections of vertex and data objects.
   *
   * @param graphDataCollection   collection of graph heads
   * @param vertexDataCollection  collection of vertices
   * @param edgeDataCollection    collection of edges
   * @param config                Gradoop Flink configuration
   * @param <G>                   EPGM graph head type
   * @param <V>                   EPGM vertex type
   * @param <E>                   EPGM edge type
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge>
  EPGMDatabase fromCollection(
    Collection<G> graphDataCollection,
    Collection<V> vertexDataCollection,
    Collection<E> edgeDataCollection,
    GradoopFlinkConfig<G, V, E> config) {
    if (config == null) {
      throw new IllegalArgumentException("Config must not be null");
    }
    ExecutionEnvironment env = config.getExecutionEnvironment();
    DataSet<V> vertices = env.fromCollection(vertexDataCollection);
    DataSet<E> edges = env.fromCollection(edgeDataCollection);
    DataSet<G> graphHeads;
    if (graphDataCollection != null) {
      graphHeads = env.fromCollection(graphDataCollection);
    } else {
      graphHeads = env.fromElements(
        config.getGraphHeadFactory().createGraphHead());
    }

    return new EPGMDatabase<>(graphHeads, vertices, edges, config);
  }

  /**
   * Creates an EPGM database from an EPGM Store using the given arguments.
   *
   * @param epgmStore EPGM store
   * @param env       Flink execution environment
   * @param <G>       graph data type
   * @param <V>       vertex data type
   * @param <E>       edge data type
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> EPGMDatabase<G, V, E> fromHBase(
    EPGMStore<G, V, E> epgmStore, ExecutionEnvironment env) {

    GradoopConfig<G, V, E> conf = epgmStore.getConfig();
    // used for type hinting when loading vertex data
    TypeInformation<Vertex<GradoopId, V>> vertexTypeInfo =
      new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(
          epgmStore.getConfig().getVertexFactory().getType()));
    // used for type hinting when loading edge data
    TypeInformation<Edge<GradoopId, E>> edgeTypeInfo =
      new TupleTypeInfo(Edge.class, BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(
          conf.getEdgeHandler().getEdgeFactory().getType()));
    // used for type hinting when loading graph data
    TypeInformation<Subgraph<GradoopId, G>> graphTypeInfo =
      new TupleTypeInfo(Subgraph.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(
          conf.getGraphHeadHandler().getGraphHeadFactory().getType()));

    DataSet<Vertex<GradoopId, V>> vertexDataSet = env.createInput(
      new HBaseReader.VertexTableInputFormat<>(
        conf.getVertexHandler(), epgmStore.getVertexTableName()),
      vertexTypeInfo);

    DataSet<Edge<GradoopId, E>> edgeDataSet = env.createInput(
      new HBaseReader.EdgeTableInputFormat<>(conf.getEdgeHandler(),
        epgmStore.getEdgeTableName()), edgeTypeInfo);

    DataSet<Subgraph<GradoopId, G>> subgraphDataSet = env.createInput(
      new HBaseReader.GraphHeadTableInputFormat<>(
        conf.getGraphHeadHandler(), epgmStore.getGraphHeadName()),
      graphTypeInfo);

    return new EPGMDatabase<>(
      subgraphDataSet.map(new MapFunction<Subgraph<GradoopId, G>, G>() {
        @Override
        public G map(Subgraph<GradoopId, G> longGDSubgraph) throws Exception {
          return longGDSubgraph.getValue();
        }
      }).withForwardedFields("f1->*"),
      vertexDataSet.map(new MapFunction<Vertex<GradoopId, V>, V>() {
        @Override
        public V map(Vertex<GradoopId, V> longVDVertex) throws Exception {
          return longVDVertex.getValue();
        }
      }).withForwardedFields("f1->*"),
      edgeDataSet.map(new MapFunction<Edge<GradoopId, E>, E>() {
        @Override
        public E map(Edge<GradoopId, E> longEDEdge) throws Exception {
          return longEDEdge.getValue();
        }
      }).withForwardedFields("f2->*"),
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
      .fromDataSets(graphHead, database.getVertices(), database.getEdges(),
        config);
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

    return GraphCollection.fromDataSets(database.getGraphHeads(), newVertices,
      newEdges, config);
  }
}
