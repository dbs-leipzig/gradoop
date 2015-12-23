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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.hbase;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.gradoop.io.hbase.functions.EdgeSetBySourceId;
import org.gradoop.io.hbase.functions.EdgeSetByTargetId;
import org.gradoop.io.hbase.functions.BuildPersistentEdge;
import org.gradoop.io.hbase.functions.BuildPersistentGraphHead;
import org.gradoop.io.hbase.functions.BuildPersistentVertex;
import org.gradoop.io.hbase.functions.BuildEdgeMutation;
import org.gradoop.io.hbase.functions.BuildGraphHeadMutation;
import org.gradoop.io.hbase.functions.BuildVertexMutation;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.LogicalGraph;
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
 * Converts runtime representation of EPGM elements into persistent
 * representations and writes them to HBase.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseWriter<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge> implements Serializable {

  /**
   * Converts runtime vertex data to persistent vertex data (includes
   * incoming and outgoing edge data) and writes it to HBase.
   *
   * @param epgmDatabase            EPGM database instance
   * @param vertexHandler           vertex data handler
   * @param persistentVertexFactory persistent vertex data factory
   * @param vertexDataTableName     HBase vertex data table name
   * @param <PVD>                   persistent vertex data type
   * @throws Exception
   */
  public <PVD extends PersistentVertex<E>> void writeVertices(
    final EPGMDatabase<G, V, E> epgmDatabase,
    final VertexHandler<V, E> vertexHandler,
    final PersistentVertexFactory<V, E, PVD> persistentVertexFactory,
    final String vertexDataTableName) throws Exception {

    final LogicalGraph<G, V, E> graph = epgmDatabase.getDatabaseGraph();

    // group edges by source vertex id (vertex-id, [out-edge])
    DataSet<Tuple2<GradoopId, Set<E>>> vertexToOutgoingEdges =
      graph.getEdges()
        .groupBy(new SourceId<E>())
        .reduceGroup(new EdgeSetBySourceId<E>());

    // group edges by target vertex id (vertex-id, [in-edge])
    DataSet<Tuple2<GradoopId, Set<E>>> vertexToIncomingEdges =
      graph.getEdges()
        .groupBy(new TargetId<E>())
        .reduceGroup(new EdgeSetByTargetId<E>());

    // co-group (vertex-data) with (vertex-id, [out-edge])
    DataSet<Tuple2<V, Set<E>>> vertexDataWithOutgoingEdges = graph.getVertices()
      .coGroup(vertexToOutgoingEdges)
      .where(new Id<V>()).equalTo(0)
      .with(new CoGroupFunction
        <V, Tuple2<GradoopId, Set<E>>, Tuple2<V, Set<E>>>() {

        @Override
        public void coGroup(Iterable<V> vertexIterable,
          Iterable<Tuple2<GradoopId, Set<E>>> outEdgesIterable,
          Collector<Tuple2<V, Set<E>>> collector) throws Exception {
          // read vertex from left group
          V vertex = vertexIterable.iterator().next();
          Set<E> outgoingEdgeData = null;

          // read outgoing edge from right group (may be empty)
          for (Tuple2<GradoopId, Set<E>> oEdges : outEdgesIterable) {
            outgoingEdgeData = oEdges.f1;
          }
          collector.collect(new Tuple2<>(vertex, outgoingEdgeData));
        }
      });

    // co-group
    // (vertex, (vertex-id, [out-edge])) with (vertex-id, [in-edge])
    DataSet<PersistentVertex<E>> persistentVertexDataSet =
      vertexDataWithOutgoingEdges
        .coGroup(vertexToIncomingEdges)
        .where("f0.id").equalTo(0)
        .with(new BuildPersistentVertex<>(persistentVertexFactory));

    // write (persistent-vertex-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, vertexDataTableName);

    persistentVertexDataSet
      .map(new BuildVertexMutation<>(vertexHandler))
      .output(
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
  public <PED extends PersistentEdge<V>> void writeEdges(
    final EPGMDatabase<G, V, E> epgmDatabase,
    final EdgeHandler<V, E> edgeHandler,
    final PersistentEdgeFactory<V, E, PED> persistentEdgeFactory,
    final String edgeDataTableName) throws IOException {

    LogicalGraph<G, V, E> graph = epgmDatabase.getDatabaseGraph();

    DataSet<PersistentEdge<V>> persistentEdgeDataSet = graph.getVertices()
      // join vertex with edges on edge source vertex id
      .join(graph.getEdges())
      .where(new Id<V>())
      .equalTo(new SourceId<E>())
      // join result with vertices on edge target vertex id
      .join(graph.getVertices())
      .where("f1.targetId")
      .equalTo(new Id<V>())
      // ((source-vertex-data, edge-data), target-vertex-data)
      .with(new BuildPersistentEdge<>(persistentEdgeFactory));

    // write (persistent-edge-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, edgeDataTableName);

    persistentEdgeDataSet
      .map(new BuildEdgeMutation<>(edgeHandler))
      .output(
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
    final EPGMDatabase<G, V, E> epgmDatabase,
    final GraphHeadHandler<G> graphHeadHandler,
    final PersistentGraphHeadFactory<G, PGD> persistentGraphHeadFactory,
    final String graphDataTableName) throws IOException {
    final LogicalGraph<G, V, E> graph = epgmDatabase.getDatabaseGraph();

    // build (graph-id, vertex-id) tuples from vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphIdToVertexId =
      graph.getVertices()
        .flatMap(new FlatMapFunction<V, Tuple2<GradoopId, GradoopId>>() {
          @Override
          public void flatMap(V vertex,
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
        .flatMap(new FlatMapFunction<E, Tuple2<GradoopId, GradoopId>>() {
          @Override
          public void flatMap(E edge,
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
        .where(0).equalTo(new Id<G>())
        .with(
          new BuildPersistentGraphHead<>(persistentGraphHeadFactory));

    // write (persistent-graph-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, graphDataTableName);

    persistentGraphDataSet
      .map(new BuildGraphHeadMutation<>(graphHeadHandler)).
      output(
        new HadoopOutputFormat<>(new TableOutputFormat<GradoopId>(), job));
  }
}
