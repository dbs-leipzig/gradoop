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

package org.gradoop.io.impl.hbase;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.impl.hbase.functions.BuildEdgeMutation;
import org.gradoop.io.impl.hbase.functions.BuildGraphHeadMutation;
import org.gradoop.io.impl.hbase.functions.BuildGraphTransactions;
import org.gradoop.io.impl.hbase.functions.BuildPersistentEdge;
import org.gradoop.io.impl.hbase.functions.BuildPersistentGraphHead;
import org.gradoop.io.impl.hbase.functions.BuildPersistentVertex;
import org.gradoop.io.impl.hbase.functions.BuildVertexDataWithEdges;
import org.gradoop.io.impl.hbase.functions.BuildVertexMutation;
import org.gradoop.io.impl.hbase.functions.EdgeSetBySourceId;
import org.gradoop.io.impl.hbase.functions.EdgeSetByTargetId;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.epgm.TargetId;
import org.gradoop.model.impl.functions.graphcontainment.PairGraphIdWithElementId;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Set;

/**
 * Converts runtime representation of EPGM elements into persistent
 * representations and writes them to HBase.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseDataSink
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends HBaseBase<G, V, E>
  implements DataSink<G, V, E> {

  /**
   * Creates a new HBase data sink.
   *
   * @param epgmStore store implementation
   * @param config    Gradoop Flink configuration
   */
  public HBaseDataSink(HBaseEPGMStore<G, V, E> epgmStore,
    GradoopFlinkConfig<G, V, E> config) {
    super(epgmStore, config);
  }

  @Override
  public void write(LogicalGraph<G, V, E> logicalGraph) throws IOException {
    write(GraphCollection.fromGraph(logicalGraph));
  }

  @Override
  public void write(GraphCollection<G, V, E> graphCollection) throws
    IOException {

    // transform graph data to persistent graph data and write it
    writeGraphHeads(graphCollection);

    // transform vertex data to persistent vertex data and write it
    writeVertices(graphCollection);

    // transform edge data to persistent edge data and write it
    writeEdges(graphCollection);
  }

  @Override
  public void write(GraphTransactions<G, V, E> graphTransactions) throws
    IOException {
    write(GraphCollection.fromTransactions(graphTransactions));
  }

  /**
   * Converts runtime graph data to persistent graph data (including vertex
   * and edge identifiers) and writes it to HBase.
   *
   * @param collection Graph collection
   * @throws IOException
   */
  private void writeGraphHeads(final GraphCollection<G, V, E> collection) throws
    IOException {

    // build (graph-id, vertex-id) tuples from vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphIdToVertexId =
      collection.getVertices().flatMap(new PairGraphIdWithElementId<V>());

    // build (graph-id, edge-id) tuples from vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphIdToEdgeId =
      collection.getEdges().flatMap(new PairGraphIdWithElementId<E>());

    // co-group (graph-id, vertex-id) and (graph-id, edge-id) tuples to
    // (graph-id, {vertex-id}, {edge-id}) triples
    DataSet<Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>>
      graphToVertexIdsAndEdgeIds = graphIdToVertexId
        .coGroup(graphIdToEdgeId)
        .where(0)
        .equalTo(0)
        .with(new BuildGraphTransactions());

    // join (graph-id, {vertex-id}, {edge-id}) triples with
    // (graph-id, graph-data) and build (persistent-graph-data)
    DataSet<PersistentGraphHead> persistentGraphDataSet =
      graphToVertexIdsAndEdgeIds
        .join(collection.getGraphHeads())
        .where(0).equalTo(new Id<G>())
        .with(new BuildPersistentGraphHead<>(
          getHBaseConfig().getPersistentGraphHeadFactory()));

    // write (persistent-graph-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration().set(
      TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getGraphTableName());

    persistentGraphDataSet
      .map(new BuildGraphHeadMutation<>(getFlinkConfig().getGraphHeadHandler()))
      .output(new HadoopOutputFormat<>(
        new TableOutputFormat<GradoopId>(), job));
  }

  /**
   * Converts runtime vertex data to persistent vertex data (includes
   * incoming and outgoing edge data) and writes it to HBase.
   *
   * @param collection Graph collection
   * @throws IOException
   */
  private void writeVertices(final GraphCollection<G, V, E> collection) throws
    IOException {

    // group edges by source vertex id (vertex-id, [out-edge])
    DataSet<Tuple2<GradoopId, Set<E>>> vertexToOutgoingEdges =
      collection.getEdges()
        .groupBy(new SourceId<E>())
        .reduceGroup(new EdgeSetBySourceId<E>());

    // group edges by target vertex id (vertex-id, [in-edge])
    DataSet<Tuple2<GradoopId, Set<E>>> vertexToIncomingEdges =
      collection.getEdges()
        .groupBy(new TargetId<E>())
        .reduceGroup(new EdgeSetByTargetId<E>());

    // co-group (vertex-data) with (vertex-id, [out-edge])
    DataSet<Tuple2<V, Set<E>>> vertexDataWithOutgoingEdges = collection
      .getVertices()
      .coGroup(vertexToOutgoingEdges)
      .where(new Id<V>()).equalTo(0)
      .with(new BuildVertexDataWithEdges<V, E>());

    // co-group
    // (vertex, (vertex-id, [out-edge])) with (vertex-id, [in-edge])
    DataSet<PersistentVertex<E>> persistentVertexDataSet =
      vertexDataWithOutgoingEdges
        .coGroup(vertexToIncomingEdges)
        .where("f0.id").equalTo(0)
        .with(new BuildPersistentVertex<>(
          getHBaseConfig().getPersistentVertexFactory()));

    // write (persistent-vertex-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration().set(
      TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getVertexTableName());

    persistentVertexDataSet
      .map(new BuildVertexMutation<>(getFlinkConfig().getVertexHandler()))
      .output(
        new HadoopOutputFormat<>(new TableOutputFormat<GradoopId>(), job));
  }

  /**
   * Converts runtime edge data to persistent edge data (includes
   * source/target vertex data) and writes it to HBase.
   *
   * @param collection Graph collection
   * @throws IOException
   */
  private void writeEdges(final GraphCollection<G, V, E> collection) throws
    IOException {

    DataSet<PersistentEdge<V>> persistentEdgeDataSet = collection.getVertices()
      // join vertex with edges on edge source vertex id
      .join(collection.getEdges())
      .where(new Id<V>())
      .equalTo(new SourceId<E>())
      // join result with vertices on edge target vertex id
      .join(collection.getVertices())
      .where("f1.targetId")
      .equalTo(new Id<V>())
      // ((source-vertex-data, edge-data), target-vertex-data)
      .with(new BuildPersistentEdge<>(
        getHBaseConfig().getPersistentEdgeFactory()));

    // write (persistent-edge-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getEdgeTableName());

    persistentEdgeDataSet
      .map(new BuildEdgeMutation<>(getFlinkConfig().getEdgeHandler()))
      .output(new HadoopOutputFormat<>(
        new TableOutputFormat<GradoopId>(), job));
  }
}
