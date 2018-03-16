/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.hbase.functions.BuildGraphHeadMutation;
import org.gradoop.flink.io.impl.hbase.functions.BuildGraphTransactions;
import org.gradoop.flink.io.impl.hbase.functions.BuildPersistentEdge;
import org.gradoop.flink.io.impl.hbase.functions.BuildPersistentGraphHead;
import org.gradoop.flink.io.impl.hbase.functions.BuildPersistentVertex;
import org.gradoop.flink.io.impl.hbase.functions.BuildVertexDataWithEdges;
import org.gradoop.flink.io.impl.hbase.functions.BuildVertexMutation;
import org.gradoop.flink.io.impl.hbase.functions.EdgeSetBySourceId;
import org.gradoop.flink.io.impl.hbase.functions.EdgeSetByTargetId;
import org.gradoop.flink.io.impl.hbase.functions.BuildEdgeMutation;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
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
    write(logicalGraph, false);
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(graphCollection, false);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {
    write(getFlinkConfig().getGraphCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {

    // TODO: interpret overWrite

    // transform graph data to persistent graph data and write it
    writeGraphHeads(graphCollection);

    // transform vertex data to persistent vertex data and write it
    writeVertices(graphCollection);

    // transform edge data to persistent edge data and write it
    writeEdges(graphCollection);
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
    DataSet<Tuple2<GradoopId, GradoopId>> graphIdToVertexId = collection.getVertices()
      .flatMap(new PairGraphIdWithElementId<>());

    // build (graph-id, edge-id) tuples from vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphIdToEdgeId = collection.getEdges()
      .flatMap(new PairGraphIdWithElementId<>());

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
    DataSet<PersistentGraphHead> persistentGraphDataSet = graphToVertexIdsAndEdgeIds
      .join(collection.getGraphHeads())
      .where(0).equalTo(new Id<>())
      .with(new BuildPersistentGraphHead<>(getHBaseConfig().getPersistentGraphHeadFactory()));

    // write (persistent-graph-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration().set(
      TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getGraphTableName());

    persistentGraphDataSet
    // FIXME remove forced cast...
      .map(new BuildGraphHeadMutation((GraphHeadHandler<PersistentGraphHead>)
((Object) getHBaseConfig().getGraphHeadHandler())))
      .output(new HadoopOutputFormat<>(new TableOutputFormat<>(), job));
  }

  /**
   * Converts runtime vertex data to persistent vertex data (includes
   * incoming and outgoing edge data) and writes it to HBase.
   *
   * @param collection Graph collection
   * @throws IOException
   */
  private void writeVertices(final GraphCollection collection) throws IOException {

    // group edges by source vertex id (vertex-id, [out-edge])
    DataSet<Tuple2<GradoopId, Set<Edge>>> vertexToOutgoingEdges = collection.getEdges()
      .groupBy(new SourceId<>())
      .reduceGroup(new EdgeSetBySourceId<>());

    // group edges by target vertex id (vertex-id, [in-edge])
    DataSet<Tuple2<GradoopId, Set<Edge>>> vertexToIncomingEdges = collection.getEdges()
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
    DataSet<PersistentVertex<Edge>> persistentVertexDataSet = vertexDataWithOutgoingEdges
      .coGroup(vertexToIncomingEdges)
      .where("f0.id").equalTo(0)
      .with(new BuildPersistentVertex<>(getHBaseConfig().getPersistentVertexFactory()));

    // write (persistent-vertex-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getVertexTableName());

    persistentVertexDataSet
      .map(new BuildVertexMutation<>(getHBaseConfig().getVertexHandler()))
      .output(new HadoopOutputFormat<>(new TableOutputFormat<>(), job));
  }

  /**
   * Converts runtime edge data to persistent edge data (includes
   * source/target vertex data) and writes it to HBase.
   *
   * @param collection Graph collection
   * @throws IOException
   */
  private void writeEdges(final GraphCollection collection) throws IOException {

    DataSet<PersistentEdge<Vertex>> persistentEdgeDataSet = collection
      .getVertices()
      // join vertex with edges on edge source vertex id
      .join(collection.getEdges())
      .where(new Id<>())
      .equalTo(new SourceId<>())
      // join result with vertices on edge target vertex id
      .join(collection.getVertices())
      .where("f1.targetId")
      .equalTo(new Id<>())
      // ((source-vertex-data, edge-data), target-vertex-data)
      .with(new BuildPersistentEdge<>(getHBaseConfig().getPersistentEdgeFactory()));

    // write (persistent-edge-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getEdgeTableName());

    persistentEdgeDataSet
      .map(new BuildEdgeMutation<>(getHBaseConfig().getEdgeHandler()))
      .output(new HadoopOutputFormat<>(new TableOutputFormat<>(), job));
  }
}
