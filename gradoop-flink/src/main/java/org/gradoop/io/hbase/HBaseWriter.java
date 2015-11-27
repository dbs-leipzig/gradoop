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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.model.EPGMDatabase;
import org.gradoop.model.impl.model.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.epgm.TargetId;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.storage.api.EdgeHandler;
import org.gradoop.storage.api.GraphHeadHandler;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentEdgeFactory;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentGraphHeadFactory;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;
import org.gradoop.storage.api.VertexHandler;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

/**
 * Contains methods and classes to write an EPGM database to HBase.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class HBaseWriter<VD extends EPGMVertex, ED extends EPGMEdge, GD
  extends EPGMGraphHead> implements Serializable {

  /**
   * Converts runtime vertex data to persistent vertex data (includes
   * incoming and outgoing edge data) and writes it to HBase.
   *
   * @param epgmDatabase                EPGM database instance
   * @param vertexHandler           vertex data handler
   * @param persistentVertexFactory persistent vertex data factory
   * @param vertexDataTableName         HBase vertex data table name
   * @param <PVD>                       persistent vertex data type
   * @throws Exception
   */
  public <PVD extends PersistentVertex<ED>> void writeVertices(
    final EPGMDatabase<GD, VD, ED> epgmDatabase,
    final VertexHandler<VD, ED> vertexHandler,
    final PersistentVertexFactory<VD, ED, PVD> persistentVertexFactory,
    final String vertexDataTableName) throws Exception {

    final LogicalGraph<GD, VD, ED> graph = epgmDatabase.getDatabaseGraph();

    // group edges by source vertex id (vertex-id, [out-edge-data])
    DataSet<Tuple2<GradoopId, Set<ED>>> vertexToOutgoingEdges = graph.getEdges()
      .groupBy(new SourceId<ED>())
      .reduceGroup(new GroupReduceFunction<ED, Tuple2<GradoopId, Set<ED>>>() {
        @Override
        public void reduce(Iterable<ED> edgeIterable,
          Collector<Tuple2<GradoopId, Set<ED>>> collector) throws Exception {
          Set<ED> outgoingEdgeData = Sets.newHashSet();
          GradoopId vertexId = null;
          boolean initialized = false;
          for (ED edge : edgeIterable) {
            if (!initialized) {
              vertexId = edge.getSourceId();
              initialized = true;
            }
            outgoingEdgeData.add(edge);
          }
          collector.collect(new Tuple2<>(vertexId, outgoingEdgeData));
        }
      });

    // group edges by target vertex id (vertex-id, [in-edge-data])
    DataSet<Tuple2<GradoopId, Set<ED>>> vertexToIncomingEdges = graph.getEdges()
      .groupBy(new TargetId<ED>())
      .reduceGroup(new GroupReduceFunction<ED, Tuple2<GradoopId, Set<ED>>>() {
        @Override
        public void reduce(Iterable<ED> edgeIterable,
          Collector<Tuple2<GradoopId, Set<ED>>> collector) throws Exception {
          Set<ED> outgoingEdgeData = Sets.newHashSet();
          GradoopId vertexId = null;
          boolean initialized = false;
          for (ED edge : edgeIterable) {
            if (!initialized) {
              vertexId = edge.getTargetId();
              initialized = true;
            }
            outgoingEdgeData.add(edge);
          }
          collector.collect(new Tuple2<>(vertexId, outgoingEdgeData));
        }
      });

    // co-group (vertex-data) with (vertex-id, [out-edge-data]) to simulate left
    // outer join
    DataSet<Tuple2<VD, Set<ED>>> vertexDataWithOutgoingEdges =
      graph.getVertices()
        .coGroup(vertexToOutgoingEdges)
        .where("id")
        .equalTo(0)
        .with(
          new CoGroupFunction<VD, Tuple2<GradoopId, Set<ED>>,
            Tuple2<VD, Set<ED>>>() {
            @Override
            public void coGroup(Iterable<VD> vertexIterable,
              Iterable<Tuple2<GradoopId, Set<ED>>> outEdgesIterable,
              Collector<Tuple2<VD, Set<ED>>> collector) throws
              Exception {
              VD vertex = null;
              Set<ED> outgoingEdgeData = null;
              // read vertex data from left group
              for (VD v : vertexIterable) {
                vertex = v;
              }
              // read outgoing edge from right group (may be empty)
              for (Tuple2<GradoopId, Set<ED>> oEdges : outEdgesIterable) {
                outgoingEdgeData = oEdges.f1;
              }
              collector.collect(new Tuple2<>(vertex, outgoingEdgeData));
            }
          });

    // co-group (vertex-data, (vertex-id, [out-edge-data])) with (vertex-id,
    // [in-edge-data]) to simulate left outer join
    DataSet<PersistentVertex<ED>> persistentVertexDataSet =
      vertexDataWithOutgoingEdges
        .coGroup(vertexToIncomingEdges)
        .where(new KeySelector<Tuple2<VD, Set<ED>>, GradoopId>() {
          @Override
          public GradoopId getKey(Tuple2<VD, Set<ED>> vdSetTuple2
          ) throws Exception {
            return vdSetTuple2.f0.getId();
          }
        })
        .equalTo(0)
        .with(new PersistentVertexCoGroupFunction<>(persistentVertexFactory));

    // write (persistent-vertex-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, vertexDataTableName);

    persistentVertexDataSet
      .map(new VertexToHBaseMapper<>(vertexHandler)).
      output(
        new HadoopOutputFormat<>(new TableOutputFormat<GradoopId>(), job));
  }

  /**
   * Converts runtime edge data to persistent edge data (includes
   * source/target vertex data) and writes it to HBase.
   *
   * @param epgmDatabase              EPGM database instance
   * @param edgeHandler           edge data handler
   * @param persistentEdgeFactory persistent edge data factory
   * @param edgeDataTableName         HBase edge data table name
   * @param <PED>                     persistent edge data type
   * @throws IOException
   */
  public <PED extends PersistentEdge<VD>> void writeEdges(
    final EPGMDatabase<GD, VD, ED> epgmDatabase,
    final EdgeHandler<ED, VD> edgeHandler,
    final PersistentEdgeFactory<ED, VD, PED> persistentEdgeFactory,
    final String edgeDataTableName) throws IOException {

    LogicalGraph<GD, VD, ED> graph = epgmDatabase.getDatabaseGraph();

    DataSet<PersistentEdge<VD>> persistentEdgeDataSet = graph.getVertices()
      // join vertex with edges on edge source vertex id
      .join(graph.getEdges())
      .where(new Id<VD>())
      .equalTo(new SourceId<ED>())
      // join result with vertices on edge target vertex id
      .join(graph.getVertices())
      .where("f1.targetVertexId")
      .equalTo(new Id<VD>())
      // ((source-vertex-data, edge-data), target-vertex-data)
      .with(new PersistentEdgeJoinFunction<>(persistentEdgeFactory));

    // write (persistent-edge-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, edgeDataTableName);

    persistentEdgeDataSet
      .map(new EdgeToHBaseMapper<>(edgeHandler)).
      output(
        new HadoopOutputFormat<>(new TableOutputFormat<GradoopId>(), job));
  }

  /**
   * Converts runtime graph data to persistent graph data (including vertex
   * and edge identifiers) and writes it to HBase.
   *
   * @param epgmDatabase               EPGM database instance
   * @param graphHeadHandler           graph data handler
   * @param persistentGraphHeadFactory persistent graph data factory
   * @param graphDataTableName         HBase graph data table name
   * @param <PGD>                      persistent graph data type
   * @throws IOException
   */
  public <PGD extends PersistentGraphHead> void writeGraphHeads(
    final EPGMDatabase<GD, VD, ED> epgmDatabase,
    final GraphHeadHandler<GD> graphHeadHandler,
    final PersistentGraphHeadFactory<GD, PGD> persistentGraphHeadFactory,
    final String graphDataTableName) throws IOException {
    final LogicalGraph<GD, VD, ED> graph = epgmDatabase.getDatabaseGraph();

    // build (graph-id, vertex-id) tuples from vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphIdToVertexId =
      graph.getVertices()
        .flatMap(new FlatMapFunction<VD, Tuple2<GradoopId, GradoopId>>() {
          @Override
          public void flatMap(VD vertex,
            Collector<Tuple2<GradoopId, GradoopId>> collector
          ) throws Exception {

            if (vertex.getGraphCount() > 0) {
              for (GradoopId graphID : vertex.getGraphIds()) {
                collector.collect(new Tuple2<>(graphID, vertex.getId()));
              }
            }
          }
        });

    // build (graph-id, edge-id) tuples from vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphIdToEdgeId =
      graph.getEdges()
        .flatMap(new FlatMapFunction<ED, Tuple2<GradoopId, GradoopId>>() {
          @Override
          public void flatMap(ED edge,
            Collector<Tuple2<GradoopId, GradoopId>> collector
          ) throws Exception {

            if (edge.getGraphCount() > 0) {
              for (GradoopId graphId : edge.getGraphIds()) {
                collector
                  .collect(new Tuple2<>(graphId, edge.getId()));
              }
            }
          }
        });

    // co-group (graph-id, vertex-id) and (graph-id, edge-id) tuples to
    // (graph-id, {vertex-id}, {edge-id}) triples
    DataSet<Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>>
      graphToVertexIdsAndEdgeIds = graphIdToVertexId
        .coGroup(graphIdToEdgeId)
        .where(0)
        .equalTo(0)
        .with(
          new CoGroupFunction<Tuple2<GradoopId, GradoopId>,
            Tuple2<GradoopId, GradoopId>,
            Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>>() {

            @Override
            public void coGroup(Iterable<Tuple2<GradoopId, GradoopId>>
              graphToVertexIds, Iterable<Tuple2<GradoopId, GradoopId>>
              graphToEdgeIds, Collector
              <Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>> collector
            ) throws Exception {

              GradoopIdSet vertexIds = new GradoopIdSet();
              GradoopIdSet edgeIds = new GradoopIdSet();
              GradoopId graphId = null;
              boolean initialized = false;
              for (Tuple2<GradoopId, GradoopId>
                graphToVertexTuple : graphToVertexIds) {

                if (!initialized) {
                  graphId = graphToVertexTuple.f0;
                  initialized = true;
                }
                vertexIds.add(graphToVertexTuple.f1);
              }
              for (Tuple2<GradoopId, GradoopId>
                graphToEdgeTuple : graphToEdgeIds) {

                edgeIds.add(graphToEdgeTuple.f1);
              }
              collector.collect(new Tuple3<>(graphId, vertexIds, edgeIds));
            }
          });

    // join (graph-id, {vertex-id}, {edge-id}) triples with
    // (graph-id, graph-data) and build (persistent-graph-data)
    DataSet<PersistentGraphHead> persistentGraphDataSet =
      graphToVertexIdsAndEdgeIds
        .join(epgmDatabase.getCollection().getGraphHeads())
        .where(0)
        .equalTo("id")
        .with(
          new PersistentGraphHeadJoinFunction<>(persistentGraphHeadFactory));

    // write (persistent-graph-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, graphDataTableName);

    persistentGraphDataSet
      .map(new GraphHeadToHBaseMapper<>(graphHeadHandler)).
      output(
        new HadoopOutputFormat<>(new TableOutputFormat<GradoopId>(), job));
  }

  /**
   * Used to create persistent vertex data from vertex data and
   * outgoing/incoming edge data.
   *
   * @param <VD>  EPGM vertex type
   * @param <ED>  EPGM edge type
   * @param <PVD> EPGM persistent vertex type
   */
  public static class PersistentVertexCoGroupFunction<VD extends EPGMVertex,
    ED extends EPGMEdge, PVD extends PersistentVertex<ED>> implements
    CoGroupFunction<Tuple2<VD, Set<ED>>, Tuple2<GradoopId, Set<ED>>,
      PersistentVertex<ED>> {

    /**
     * Persistent vertex data factory.
     */
    private final PersistentVertexFactory<VD, ED, PVD> vertexFactory;

    /**
     * Creates co group function.
     *
     * @param vertexFactory persistent vertex data factory
     */
    public PersistentVertexCoGroupFunction(
      PersistentVertexFactory<VD, ED, PVD> vertexFactory) {
      this.vertexFactory = vertexFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void coGroup(Iterable<Tuple2<VD, Set<ED>>> iterable,
      Iterable<Tuple2<GradoopId, Set<ED>>> iterable1,
      Collector<PersistentVertex<ED>> collector) throws Exception {
      VD vertex = null;
      Set<ED> outgoingEdgeData = null;
      Set<ED> incomingEdgeData = null;
      for (Tuple2<VD, Set<ED>> left : iterable) {
        vertex = left.f0;
        outgoingEdgeData = left.f1;
      }
      for (Tuple2<GradoopId, Set<ED>> right : iterable1) {
        incomingEdgeData = right.f1;
      }
      assert vertex != null;
      collector.collect(vertexFactory
        .createVertex(vertex, outgoingEdgeData, incomingEdgeData));
    }
  }

  /**
   * Creates persistent edge data objects from edge data and source/target
   * vertex data
   *
   * @param <ED>  EPGM edge type
   * @param <VD>  EPGM vertex type
   * @param <PED> EPGM persistent edge type
   */
  public static class PersistentEdgeJoinFunction<ED extends EPGMEdge, VD
    extends EPGMVertex, PED extends PersistentEdge<VD>> implements
    JoinFunction<Tuple2<VD, ED>, VD, PersistentEdge<VD>> {

    /**
     * Persistent edge data factory.
     */
    private final PersistentEdgeFactory<ED, VD, PED> edgeFactory;

    /**
     * Creates join function
     *
     * @param edgeFactory persistent edge data factory.
     */
    public PersistentEdgeJoinFunction(
      PersistentEdgeFactory<ED, VD, PED> edgeFactory) {
      this.edgeFactory = edgeFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersistentEdge<VD> join(
      Tuple2<VD, ED> sourceVertexAndEdge, VD targetVertex) throws Exception {
      return edgeFactory.createEdge(sourceVertexAndEdge.f1,
        sourceVertexAndEdge.f0, targetVertex);
    }
  }

  /**
   * Creates persistent graph data from graph data and vertex/edge identifiers.
   *
   * @param <GD>  EPGM graph head type
   * @param <PGD> EPGM persistent graph head type
   */
  public static class PersistentGraphHeadJoinFunction<GD extends EPGMGraphHead,
    PGD extends PersistentGraphHead> implements JoinFunction
    <Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>, GD, PersistentGraphHead> {

    /**
     * Persistent graph data factory.
     */
    private PersistentGraphHeadFactory<GD, PGD> graphHeadFactory;

    /**
     * Creates join function.
     *
     * @param graphHeadFactory persistent graph data factory
     */
    public PersistentGraphHeadJoinFunction(
      PersistentGraphHeadFactory<GD, PGD> graphHeadFactory) {
      this.graphHeadFactory = graphHeadFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersistentGraphHead join(
      Tuple3<GradoopId, GradoopIdSet, GradoopIdSet> longSetSetTuple3,
      GD graphHead) throws Exception {
      return graphHeadFactory.createGraphHead(graphHead, longSetSetTuple3.f1,
        longSetSetTuple3.f2);
    }
  }

  /**
   * Creates HBase {@link Mutation} from persistent graph data using graph
   * data handler.
   *
   * @param <GD>  EPGM graph head type
   * @param <PGD> EPGM persistent graph type
   */
  public static class GraphHeadToHBaseMapper<GD extends EPGMGraphHead, PGD
    extends PersistentGraphHead> extends
    RichMapFunction<PGD, Tuple2<GradoopId, Mutation>> {

    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 42L;

    /**
     * Reusable tuple for each writer.
     */
    private transient Tuple2<GradoopId, Mutation> reuseTuple;

    /**
     * Graph data handler to create Mutations.
     */
    private final GraphHeadHandler<GD> graphHeadHandler;

    /**
     * Creates rich map function.
     *
     * @param graphHeadHandler graph data handler
     */
    public GraphHeadToHBaseMapper(GraphHeadHandler<GD> graphHeadHandler) {
      this.graphHeadHandler = graphHeadHandler;
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
    public Tuple2<GradoopId, Mutation> map(PGD persistentGraphData) throws
      Exception {
      GradoopId key = persistentGraphData.getId();
      Put put =
        new Put(graphHeadHandler.getRowKey(persistentGraphData.getId()));
      put = graphHeadHandler.writeGraphHead(put, persistentGraphData);

      reuseTuple.f0 = key;
      reuseTuple.f1 = put;
      return reuseTuple;
    }
  }

  /**
   * Creates HBase {@link Mutation} from persistent vertex data using vertex
   * data handler.
   *
   * @param <VD>  EPGM vertex type
   * @param <ED>  EPGM edge type
   * @param <PVD> EPGM persistent vertex type
   */
  public static class VertexToHBaseMapper<VD extends EPGMVertex, ED
    extends EPGMEdge, PVD extends PersistentVertex<ED>> extends
    RichMapFunction<PVD, Tuple2<GradoopId, Mutation>> {

    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 42L;

    /**
     * Reusable tuple for each writer.
     */
    private transient Tuple2<GradoopId, Mutation> reuseTuple;

    /**
     * Vertex data handler to create Mutations.
     */
    private final VertexHandler<VD, ED> vertexHandler;

    /**
     * Creates rich map function.
     *
     * @param vertexHandler vertex data handler
     */
    public VertexToHBaseMapper(VertexHandler<VD, ED> vertexHandler) {
      this.vertexHandler = vertexHandler;
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
    public Tuple2<GradoopId, Mutation> map(PVD persistentVertexData) throws
      Exception {
      GradoopId key = persistentVertexData.getId();
      Put put =
        new Put(vertexHandler.getRowKey(persistentVertexData.getId()));
      put = vertexHandler.writeVertex(put, persistentVertexData);

      reuseTuple.f0 = key;
      reuseTuple.f1 = put;
      return reuseTuple;
    }
  }

  /**
   * Creates HBase {@link Mutation} from persistent edge data using edge
   * data handler.
   *
   * @param <ED>  EPGM edge type
   * @param <VD>  EPGM vertex type
   * @param <PED> EPGM persistent edge type
   */
  private static class EdgeToHBaseMapper
    <ED extends EPGMEdge, VD extends EPGMVertex, PED extends PersistentEdge<VD>>
    extends RichMapFunction<PED, Tuple2<GradoopId, Mutation>> {

    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 42L;

    /**
     * Reusable tuple for each writer.
     */
    private transient Tuple2<GradoopId, Mutation> reuseTuple;

    /**
     * Edge data handler to create Mutations.
     */
    private final EdgeHandler<ED, VD> edgeHandler;

    /**
     * Creates rich map function.
     *
     * @param edgeHandler edge data handler
     */
    public EdgeToHBaseMapper(EdgeHandler<ED, VD> edgeHandler) {
      this.edgeHandler = edgeHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple2<GradoopId, Mutation> map(PED persistentEdgeData) throws
      Exception {
      GradoopId key = persistentEdgeData.getId();
      Put put = new Put(edgeHandler.getRowKey(persistentEdgeData.getId()));
      put = edgeHandler.writeEdge(put, persistentEdgeData);

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
