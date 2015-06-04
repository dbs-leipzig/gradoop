package org.gradoop.biiig.io.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.io.formats.HBaseVertexOutputFormat;
import org.gradoop.model.Graph;
import org.gradoop.model.impl.GraphFactory;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

/**
 * Used to write resulting vertices of BTG Computation to HBase.
 */
public class BTGHBaseVertexOutputFormat extends
  HBaseVertexOutputFormat<LongWritable, BTGVertexValue, NullWritable> {

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWriter<LongWritable, BTGVertexValue, NullWritable>
  createVertexWriter(
    TaskAttemptContext context) throws IOException, InterruptedException {
    return new BTGHBaseVertexWriter(context);
  }

  /**
   * Writes a single BTG Giraph vertex back to HBase.
   */
  public static class BTGHBaseVertexWriter extends
    HBaseVertexWriter<LongWritable, BTGVertexValue, NullWritable> {

    /**
     * Used to access persistent graph database.
     */
    private GraphStore graphStore;

    /**
     * Sets up base table output format and creates a record writer.
     *
     * @param context task attempt context
     */
    public BTGHBaseVertexWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
      super(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(TaskAttemptContext context) throws IOException {
      super.initialize(context);
      VertexHandler vertexHandler = new EPGVertexHandler();
      GraphHandler graphHandler = new EPGGraphHandler();
      graphStore = HBaseGraphStoreFactory
        .createOrOpenGraphStore(context.getConfiguration(), vertexHandler,
          graphHandler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertex(
      Vertex<LongWritable, BTGVertexValue, NullWritable> vertex) throws
      IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Mutation> writer = getRecordWriter();
      VertexHandler vertexHandler = getVertexHandler();
      byte[] rowKey = vertexHandler.getRowKey(vertex.getId().get());
      Put put = new Put(rowKey);
      // update graphs
//      for (Long graphID : vertex.getValue().getGraphs()) {
//        updateGraph(graphID, vertex.getId().get());
//      }
      // update vertex
      put = vertexHandler.writeGraphs(put, vertex.getValue());
      writer.write(new ImmutableBytesWritable(rowKey), put);
    }

    /**
     * Adds the given vertex to the given graph.
     *
     * @param graphID  graph identifier
     * @param vertexID vertex identifier
     */
    private void updateGraph(Long graphID, Long vertexID) {
      Graph g = GraphFactory.createDefaultGraphWithID(graphID);
      g.addVertex(vertexID);
      graphStore.writeGraph(g);
    }
  }
}
