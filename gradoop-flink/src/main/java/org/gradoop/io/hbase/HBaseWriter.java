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

package org.gradoop.io.hbase;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.Subgraph;
import org.gradoop.storage.EdgeDataHandler;
import org.gradoop.storage.GraphDataHandler;
import org.gradoop.storage.PersistentEdgeData;
import org.gradoop.storage.PersistentEdgeDataFactory;
import org.gradoop.storage.PersistentGraphData;
import org.gradoop.storage.PersistentGraphDataFactory;
import org.gradoop.storage.PersistentVertexData;
import org.gradoop.storage.PersistentVertexDataFactory;
import org.gradoop.storage.VertexDataHandler;

import java.io.IOException;
import java.util.Set;

/**
 * Contains methods and classes to write an EPGM database to HBase.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class HBaseWriter<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> {

  /**
   * Converts runtime vertex data to persistent vertex data (includes
   * incoming and outgoing edge data) and writes it to HBase.
   *
   * @param epgmDatabase                EPGM database instance
   * @param vertexDataHandler           vertex data handler
   * @param persistentVertexDataFactory persistent vertex data factory
   * @param vertexDataTableName         HBase vertex data table name
   * @param <PVD>                       persistent vertex data type
   * @throws Exception
   */
  public <PVD extends PersistentVertexData<ED>> void writeVertexData(
    final EPGMDatabase<VD, ED, GD> epgmDatabase,
    final VertexDataHandler<VD, ED> vertexDataHandler,
    final PersistentVertexDataFactory<VD, ED, PVD> persistentVertexDataFactory,
    final String vertexDataTableName) throws Exception {

    final LogicalGraph<VD, ED, GD> graph =
      epgmDatabase.getDatabaseGraph();

    // group edges by source vertex id (vertex-id, [out-edge-data])
    GroupReduceOperator<Edge<Long, ED>, Tuple2<Long, Set<ED>>>
      vertexToOutgoingEdges =
      graph.getEdges().groupBy(0) // group by source vertex id
        .reduceGroup(
          new GroupReduceFunction<Edge<Long, ED>, Tuple2<Long, Set<ED>>>() {
            @Override
            public void reduce(Iterable<Edge<Long, ED>> edgeIterable,
              Collector<Tuple2<Long, Set<ED>>> collector) throws Exception {
              Set<ED> outgoingEdgeData = Sets.newHashSet();
              Long vertexId = null;
              boolean initialized = false;
              for (Edge<Long, ED> edgeData : edgeIterable) {
                if (!initialized) {
                  vertexId = edgeData.getSource();
                  initialized = true;
                }
                outgoingEdgeData.add(edgeData.getValue());
              }
              collector.collect(new Tuple2<>(vertexId, outgoingEdgeData));
            }
          });

    // group edges by target vertex id (vertex-id, [in-edge-data])
    GroupReduceOperator<Edge<Long, ED>, Tuple2<Long, Set<ED>>>
      vertexToIncomingEdges =
      graph.getEdges().groupBy(1) // group by target vertex id
        .reduceGroup(
          new GroupReduceFunction<Edge<Long, ED>, Tuple2<Long, Set<ED>>>() {
            @Override
            public void reduce(Iterable<Edge<Long, ED>> edgeIterable,
              Collector<Tuple2<Long, Set<ED>>> collector) throws Exception {
              Set<ED> outgoingEdgeData = Sets.newHashSet();
              Long vertexId = null;
              boolean initialized = false;
              for (Edge<Long, ED> edgeData : edgeIterable) {
                if (!initialized) {
                  vertexId = edgeData.getTarget();
                  initialized = true;
                }
                outgoingEdgeData.add(edgeData.getValue());
              }
              collector.collect(new Tuple2<>(vertexId, outgoingEdgeData));
            }
          });

    // co-group (vertex-data) with (vertex-id, [out-edge-data]) to simulate left
    // outer join
    DataSet<Tuple2<Vertex<Long, VD>, Set<ED>>> vertexDataWithOutgoingEdges =
      graph.getVertices().coGroup(vertexToOutgoingEdges).where(0).equalTo(0)
        .with(
          new CoGroupFunction<Vertex<Long, VD>, Tuple2<Long, Set<ED>>,
            Tuple2<Vertex<Long, VD>, Set<ED>>>() {
            @Override
            public void coGroup(Iterable<Vertex<Long, VD>> vertexIterable,
              Iterable<Tuple2<Long, Set<ED>>> outEdgesIterable,
              Collector<Tuple2<Vertex<Long, VD>, Set<ED>>> collector) throws
              Exception {
              Vertex<Long, VD> vertex = null;
              Set<ED> outgoingEdgeData = null;
              // read vertex data from left group
              for (Vertex<Long, VD> v : vertexIterable) {
                vertex = v;
              }
              // read outgoing edge from right group (may be empty)
              for (Tuple2<Long, Set<ED>> oEdges : outEdgesIterable) {
                outgoingEdgeData = oEdges.f1;
              }
              collector.collect(new Tuple2<>(vertex, outgoingEdgeData));
            }
          });

    // co-group (vertex-data, (vertex-id, [out-edge-data])) with (vertex-id,
    // [in-edge-data]) to simulate left outer join
    DataSet<PersistentVertexData<ED>> persistentVertexDataSet =
      vertexDataWithOutgoingEdges.coGroup(vertexToIncomingEdges).where("0.0")
        .equalTo(0).with(
        new PersistentVertexDataCoGroupFunction<>(persistentVertexDataFactory));

    // write (persistent-vertex-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, vertexDataTableName);

    persistentVertexDataSet
      .map(new HBaseWriter.VertexDataToHBaseMapper<>(vertexDataHandler)).
      output(
        new HadoopOutputFormat<>(new TableOutputFormat<LongWritable>(), job));
  }

  /**
   * Converts runtime edge data to persistent edge data (includes
   * source/target vertex data) and writes it to HBase.
   *
   * @param epgmDatabase              EPGM database instance
   * @param edgeDataHandler           edge data handler
   * @param persistentEdgeDataFactory persistent edge data factory
   * @param edgeDataTableName         HBase edge data table name
   * @param <PED>                     persistent edge data type
   * @throws IOException
   */
  public <PED extends PersistentEdgeData<VD>> void writeEdgeData(
    final EPGMDatabase<VD, ED, GD> epgmDatabase,
    final EdgeDataHandler<ED, VD> edgeDataHandler,
    final PersistentEdgeDataFactory<ED, VD, PED> persistentEdgeDataFactory,
    final String edgeDataTableName) throws IOException {

    LogicalGraph<VD, ED, GD> graph = epgmDatabase.getDatabaseGraph();

    DataSet<PersistentEdgeData<VD>> persistentEdgeDataSet = graph.getVertices()
      // join vertex with edges on edge source vertex id
      .join(graph.getEdges()).where(0).equalTo(1)
        // join result with vertices on edge target vertex id
      .join(graph.getVertices()).where("1.1").equalTo(0)
        // ((source-vertex-data, edge-data), target-vertex-data)
      .with(new PersistentEdgeDataJoinFunction<>(persistentEdgeDataFactory));

    // write (persistent-edge-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, edgeDataTableName);

    persistentEdgeDataSet
      .map(new HBaseWriter.EdgeDataToHBaseMapper<>(edgeDataHandler)).
      output(
        new HadoopOutputFormat<>(new TableOutputFormat<LongWritable>(), job));
  }

  /**
   * Converts runtime graph data to persistent graph data (including vertex
   * and edge identifiers) and writes it to HBase.
   *
   * @param epgmDatabase               EPGM database instance
   * @param graphDataHandler           graph data handler
   * @param persistentGraphDataFactory persistent graph data factory
   * @param graphDataTableName         HBase graph data table name
   * @param <PGD>                      persistent graph data type
   * @throws IOException
   */
  public <PGD extends PersistentGraphData> void writeGraphData(
    final EPGMDatabase<VD, ED, GD> epgmDatabase,
    final GraphDataHandler<GD> graphDataHandler,
    final PersistentGraphDataFactory<GD, PGD> persistentGraphDataFactory,
    final String graphDataTableName) throws IOException {
    final LogicalGraph<VD, ED, GD> graph = epgmDatabase.getDatabaseGraph();

    // build (graph-id, vertex-id) tuples from vertices
    FlatMapOperator<Vertex<Long, VD>, Tuple2<Long, Long>> graphIdToVertexId =
      graph.getVertices()
        .flatMap(new FlatMapFunction<Vertex<Long, VD>, Tuple2<Long, Long>>() {
          @Override
          public void flatMap(Vertex<Long, VD> vertex,
            Collector<Tuple2<Long, Long>> collector) throws Exception {
            if (vertex.getValue().getGraphCount() > 0) {
              for (Long graphID : vertex.getValue().getGraphs()) {
                collector.collect(new Tuple2<>(graphID, vertex.f0));
              }
            }
          }
        });

    // build (graph-id, edge-id) tuples from vertices
    FlatMapOperator<Edge<Long, ED>, Tuple2<Long, Long>> graphIdToEdgeId =
      graph.getEdges()
        .flatMap(new FlatMapFunction<Edge<Long, ED>, Tuple2<Long, Long>>() {
          @Override
          public void flatMap(Edge<Long, ED> edge,
            Collector<Tuple2<Long, Long>> collector) throws Exception {
            if (edge.getValue().getGraphCount() > 0) {
              for (Long graphId : edge.getValue().getGraphs()) {
                collector
                  .collect(new Tuple2<>(graphId, edge.getValue().getId()));
              }
            }
          }
        });

    // co-group (graph-id, vertex-id) and (graph-id, edge-id) tuples to
    // (graph-id, {vertex-id}, {edge-id}) triples
    CoGroupOperator<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple3<Long,
      Set<Long>, Set<Long>>>
      graphToVertexIdsAndEdgeIds =
      graphIdToVertexId.coGroup(graphIdToEdgeId).where(0).equalTo(0).with(
        new CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>,
          Tuple3<Long, Set<Long>, Set<Long>>>() {

          @Override
          public void coGroup(Iterable<Tuple2<Long, Long>> graphToVertexIds,
            Iterable<Tuple2<Long, Long>> graphToEdgeIds,
            Collector<Tuple3<Long, Set<Long>, Set<Long>>> collector) throws
            Exception {
            Set<Long> vertexIds = Sets.newHashSet();
            Set<Long> edgeIds = Sets.newHashSet();
            Long graphId = null;
            boolean initialized = false;
            for (Tuple2<Long, Long> graphToVertexTuple : graphToVertexIds) {
              if (!initialized) {
                graphId = graphToVertexTuple.f0;
                initialized = true;
              }
              vertexIds.add(graphToVertexTuple.f1);
            }
            for (Tuple2<Long, Long> graphToEdgeTuple : graphToEdgeIds) {
              edgeIds.add(graphToEdgeTuple.f1);
            }
            collector.collect(new Tuple3<>(graphId, vertexIds, edgeIds));
          }
        });

    // join (graph-id, {vertex-id}, {edge-id}) triples with
    // (graph-id, graph-data) and build (persistent-graph-data)
    JoinOperator.EquiJoin<Tuple3<Long, Set<Long>, Set<Long>>, Subgraph<Long,
      GD>, PersistentGraphData>
      persistentGraphDataSet = graphToVertexIdsAndEdgeIds
      .join(epgmDatabase.getCollection().getSubgraphs()).where(0).equalTo(0)
      .with(new PersistentGraphDataJoinFunction<>(persistentGraphDataFactory));

    // write (persistent-graph-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, graphDataTableName);

    persistentGraphDataSet
      .map(new HBaseWriter.GraphDataToHBaseMapper<>(graphDataHandler)).
      output(
        new HadoopOutputFormat<>(new TableOutputFormat<LongWritable>(), job));
  }

  /**
   * Used to create persistent vertex data from vertex data and
   * outgoing/incoming edge data.
   *
   * @param <VD>  vertex data type
   * @param <ED>  edge data type
   * @param <PVD> persistent vertex data type
   */
  public static class PersistentVertexDataCoGroupFunction<VD extends
    VertexData, ED extends EdgeData, PVD extends PersistentVertexData<ED>>
    implements
    CoGroupFunction<Tuple2<Vertex<Long, VD>, Set<ED>>, Tuple2<Long, Set<ED>>,
      PersistentVertexData<ED>> {

    /**
     * Persistent vertex data factory.
     */
    private final PersistentVertexDataFactory<VD, ED, PVD> vertexDataFactory;

    /**
     * Creates co group function.
     *
     * @param vertexDataFactory persistent vertex data factory
     */
    public PersistentVertexDataCoGroupFunction(
      PersistentVertexDataFactory<VD, ED, PVD> vertexDataFactory) {
      this.vertexDataFactory = vertexDataFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void coGroup(Iterable<Tuple2<Vertex<Long, VD>, Set<ED>>> iterable,
      Iterable<Tuple2<Long, Set<ED>>> iterable1,
      Collector<PersistentVertexData<ED>> collector) throws Exception {
      Vertex<Long, VD> vertex = null;
      Set<ED> outgoingEdgeData = null;
      Set<ED> incomingEdgeData = null;
      for (Tuple2<Vertex<Long, VD>, Set<ED>> left : iterable) {
        vertex = left.f0;
        outgoingEdgeData = left.f1;
      }
      for (Tuple2<Long, Set<ED>> right : iterable1) {
        incomingEdgeData = right.f1;
      }
      assert vertex != null;
      collector.collect(vertexDataFactory
        .createVertexData(vertex.f1, outgoingEdgeData, incomingEdgeData));
    }
  }

  /**
   * Creates persistent edge data objects from edge data and source/target
   * vertex data
   *
   * @param <ED>  edge data type
   * @param <VD>  vertex data type
   * @param <PED> persistent edge data type
   */
  public static class PersistentEdgeDataJoinFunction<ED extends EdgeData, VD
    extends VertexData, PED extends PersistentEdgeData<VD>> implements
    JoinFunction<Tuple2<Vertex<Long, VD>, Edge<Long, ED>>, Vertex<Long, VD>,
      PersistentEdgeData<VD>> {

    /**
     * Persistent edge data factory.
     */
    private final PersistentEdgeDataFactory<ED, VD, PED> edgeDataFactory;

    /**
     * Creates join function
     *
     * @param edgeDataFactory persistent edge data factory.
     */
    public PersistentEdgeDataJoinFunction(
      PersistentEdgeDataFactory<ED, VD, PED> edgeDataFactory) {
      this.edgeDataFactory = edgeDataFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersistentEdgeData<VD> join(
      Tuple2<Vertex<Long, VD>, Edge<Long, ED>> vertexEdgeTuple2,
      Vertex<Long, VD> longVDVertex) throws Exception {
      VD sourceVertexData = vertexEdgeTuple2.f0.getValue();
      ED edgeData = vertexEdgeTuple2.f1.getValue();
      VD targetVertexData = longVDVertex.f1;
      return edgeDataFactory
        .createEdgeData(edgeData, sourceVertexData, targetVertexData);
    }
  }

  /**
   * Creates persistent graph data from graph data and vertex/edge identifiers.
   *
   * @param <GD>  graph data
   * @param <PGD> persistent graph data
   */
  public static class PersistentGraphDataJoinFunction<GD extends GraphData,
    PGD extends PersistentGraphData> implements
    JoinFunction<Tuple3<Long, Set<Long>, Set<Long>>, Subgraph<Long, GD>,
      PersistentGraphData> {

    /**
     * Persistent graph data factory.
     */
    private PersistentGraphDataFactory<GD, PGD> graphDataFactory;

    /**
     * Creates join function.
     *
     * @param graphDataFactory persistent graph data factory
     */
    public PersistentGraphDataJoinFunction(
      PersistentGraphDataFactory<GD, PGD> graphDataFactory) {
      this.graphDataFactory = graphDataFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersistentGraphData join(
      Tuple3<Long, Set<Long>, Set<Long>> longSetSetTuple3,
      Subgraph<Long, GD> longGDSubgraph) throws Exception {

      return graphDataFactory
        .createGraphData(longGDSubgraph.f1, longSetSetTuple3.f1,
          longSetSetTuple3.f2);
    }
  }

  /**
   * Creates HBase {@link Mutation} from persistent graph data using graph
   * data handler.
   *
   * @param <GD>  graph data type
   * @param <PGD> persistent graph data type
   */
  public static class GraphDataToHBaseMapper<GD extends GraphData, PGD
    extends PersistentGraphData> extends
    RichMapFunction<PGD, Tuple2<LongWritable, Mutation>> {

    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 42L;

    /**
     * Reusable tuple for each writer.
     */
    private transient Tuple2<LongWritable, Mutation> reuseTuple;

    /**
     * Graph data handler to create Mutations.
     */
    private final GraphDataHandler<GD> graphDataHandler;

    /**
     * Creates rich map function.
     *
     * @param graphDataHandler graph data handler
     */
    public GraphDataToHBaseMapper(GraphDataHandler<GD> graphDataHandler) {
      this.graphDataHandler = graphDataHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      reuseTuple = new Tuple2<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple2<LongWritable, Mutation> map(PGD persistentGraphData) throws
      Exception {
      LongWritable key = new LongWritable(persistentGraphData.getId());
      Put put =
        new Put(graphDataHandler.getRowKey(persistentGraphData.getId()));
      put = graphDataHandler.writeGraphData(put, persistentGraphData);

      reuseTuple.f0 = key;
      reuseTuple.f1 = put;
      return reuseTuple;
    }
  }

  /**
   * Creates HBase {@link Mutation} from persistent vertex data using vertex
   * data handler.
   *
   * @param <VD>  vertex data
   * @param <ED>  edge data
   * @param <PVD> persistent vertex data
   */
  public static class VertexDataToHBaseMapper<VD extends VertexData, ED
    extends EdgeData, PVD extends PersistentVertexData<ED>> extends
    RichMapFunction<PVD, Tuple2<LongWritable, Mutation>> {

    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 42L;

    /**
     * Reusable tuple for each writer.
     */
    private transient Tuple2<LongWritable, Mutation> reuseTuple;

    /**
     * Vertex data handler to create Mutations.
     */
    private final VertexDataHandler<VD, ED> vertexDataHandler;

    /**
     * Creates rich map function.
     *
     * @param vertexDataHandler vertex data handler
     */
    public VertexDataToHBaseMapper(
      VertexDataHandler<VD, ED> vertexDataHandler) {
      this.vertexDataHandler = vertexDataHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      reuseTuple = new Tuple2<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple2<LongWritable, Mutation> map(PVD persistentVertexData) throws
      Exception {
      LongWritable key = new LongWritable(persistentVertexData.getId());
      Put put =
        new Put(vertexDataHandler.getRowKey(persistentVertexData.getId()));
      put = vertexDataHandler.writeVertexData(put, persistentVertexData);

      reuseTuple.f0 = key;
      reuseTuple.f1 = put;
      return reuseTuple;
    }
  }

  /**
   * Creates HBase {@link Mutation} from persistent edge data using edge
   * data handler.
   *
   * @param <ED>  edge data
   * @param <VD>  vertex data
   * @param <PED> persistent edge data
   */
  private static class EdgeDataToHBaseMapper<ED extends EdgeData, VD extends
    VertexData, PED extends PersistentEdgeData<VD>> extends
    RichMapFunction<PED, Tuple2<LongWritable, Mutation>> {

    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 42L;

    /**
     * Reusable tuple for each writer.
     */
    private transient Tuple2<LongWritable, Mutation> reuseTuple;

    /**
     * Edge data handler to create Mutations.
     */
    private final EdgeDataHandler<ED, VD> edgeDataHandler;

    /**
     * Creates rich map function.
     *
     * @param edgeDataHandler edge data handler
     */
    public EdgeDataToHBaseMapper(EdgeDataHandler<ED, VD> edgeDataHandler) {
      this.edgeDataHandler = edgeDataHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple2<LongWritable, Mutation> map(PED persistentEdgeData) throws
      Exception {
      LongWritable key = new LongWritable(persistentEdgeData.getId());
      Put put = new Put(edgeDataHandler.getRowKey(persistentEdgeData.getId()));
      put = edgeDataHandler.writeEdgeData(put, persistentEdgeData);

      reuseTuple.f0 = key;
      reuseTuple.f1 = put;
      return reuseTuple;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      reuseTuple = new Tuple2<>();
    }
  }
}
