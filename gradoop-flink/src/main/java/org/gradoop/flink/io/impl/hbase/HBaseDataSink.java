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

package org.gradoop.flink.io.impl.hbase;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.hbase.functions.BuildEdgeMutation;
import org.gradoop.flink.io.impl.hbase.functions.BuildGraphHeadMutation;
import org.gradoop.flink.io.impl.hbase.functions.BuildGraphTransactions;
import org.gradoop.flink.io.impl.hbase.functions.BuildPersistentEdge;
import org.gradoop.flink.io.impl.hbase.functions.BuildPersistentGraphHead;
import org.gradoop.flink.io.impl.hbase.functions.BuildPersistentVertex;
import org.gradoop.flink.io.impl.hbase.functions.BuildVertexDataWithEdges;
import org.gradoop.flink.io.impl.hbase.functions.BuildVertexMutation;
import org.gradoop.flink.io.impl.hbase.functions.EdgeSetBySourceId;
import org.gradoop.flink.io.impl.hbase.functions.EdgeSetByTargetId;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment.PairGraphIdWithElementId;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Set;

/**
 * Converts runtime representation of EPGM elements into persistent
 * representations and writes them to HBase.
 */
public class HBaseDataSink extends HBaseBase<GraphHead, Vertex, Edge>
  implements DataSink {

  /**
   * Creates a new HBase data sink.
   *
   * @param epgmStore store implementation
   * @param config    Gradoop Flink configuration
   */
  public HBaseDataSink(HBaseEPGMStore<GraphHead, Vertex, Edge> epgmStore,
    GradoopFlinkConfig config) {
    super(epgmStore, config);
  }

  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(GraphCollection.fromGraph(logicalGraph));
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {

    // transform graph data to persistent graph data and write it
    writeGraphHeads(graphCollection);

    // transform vertex data to persistent vertex data and write it
    writeVertices(graphCollection);

    // transform edge data to persistent edge data and write it
    writeEdges(graphCollection);
  }

  @Override
  public void write(GraphTransactions graphTransactions) throws IOException {
    write(GraphCollection.fromTransactions(graphTransactions));
  }

  /**
   * Converts runtime graph data to persistent graph data (including vertex
   * and edge identifiers) and writes it to HBase.
   *
   * @param collection Graph collection
   * @throws IOException
   */
  private void writeGraphHeads(final GraphCollection collection)
      throws IOException {

    // build (graph-id, vertex-id) tuples from vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphIdToVertexId =
      collection.getVertices().flatMap(new PairGraphIdWithElementId<Vertex>());

    // build (graph-id, edge-id) tuples from vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphIdToEdgeId =
      collection.getEdges().flatMap(new PairGraphIdWithElementId<Edge>());

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
        .where(0).equalTo(new Id<GraphHead>())
        .with(new BuildPersistentGraphHead<>(
          getHBaseConfig().getPersistentGraphHeadFactory()));

    // write (persistent-graph-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration().set(
      TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getGraphTableName());

    persistentGraphDataSet
      .map(new BuildGraphHeadMutation(getFlinkConfig().getGraphHeadHandler()))
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
  private void writeVertices(final GraphCollection collection)
      throws IOException {

    // group edges by source vertex id (vertex-id, [out-edge])
    DataSet<Tuple2<GradoopId, Set<Edge>>> vertexToOutgoingEdges =
      collection.getEdges()
        .groupBy(new SourceId<>())
        .reduceGroup(new EdgeSetBySourceId<>());

    // group edges by target vertex id (vertex-id, [in-edge])
    DataSet<Tuple2<GradoopId, Set<Edge>>> vertexToIncomingEdges =
      collection.getEdges()
        .groupBy(new TargetId<>())
        .reduceGroup(new EdgeSetByTargetId<>());

    // co-group (vertex-data) with (vertex-id, [out-edge])
    DataSet<Tuple2<Vertex, Set<Edge>>> vertexDataWithOutgoingEdges = collection
      .getVertices()
      .coGroup(vertexToOutgoingEdges)
      .where(new Id<Vertex>()).equalTo(0)
      .with(new BuildVertexDataWithEdges<>());

    // co-group
    // (vertex, (vertex-id, [out-edge])) with (vertex-id, [in-edge])
    DataSet<PersistentVertex<Edge>> persistentVertexDataSet =
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
  private void writeEdges(final GraphCollection collection)
      throws IOException {

    DataSet<PersistentEdge<Vertex>> persistentEdgeDataSet = collection
      .getVertices()
      // join vertex with edges on edge source vertex id
      .join(collection.getEdges())
      .where(new Id<Vertex>())
      .equalTo(new SourceId<>())
      // join result with vertices on edge target vertex id
      .join(collection.getVertices())
      .where("f1.targetId")
      .equalTo(new Id<Vertex>())
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
