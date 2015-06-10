package org.gradoop.io.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.model.impl.VertexFactory;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

/**
 * Used to write resulting vertices of BTG Computation to HBase.
 */
public class EPGLabelPropagationOutputFormat extends
  HBaseVertexOutputFormat<LongWritable, LongWritable, NullWritable> {

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWriter<LongWritable, LongWritable, NullWritable>
  createVertexWriter(
    TaskAttemptContext context) throws IOException, InterruptedException {
    return new LPHBaseVertexWriter(context);
  }

  /**
   * Writes a single Giraph vertex back to HBase.
   */
  public static class LPHBaseVertexWriter extends
    HBaseVertexWriter<LongWritable, LongWritable, NullWritable> {

    /**
     * Sets up HBase table output format and creates a record writer.
     *
     * @param context task attempt context
     */
    public LPHBaseVertexWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
      super(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertex(
      Vertex<LongWritable, LongWritable, NullWritable> vertex) throws
      IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Mutation> writer = getRecordWriter();
      VertexHandler vertexHandler = getVertexHandler();
      byte[] rowKey = vertexHandler.getRowKey(vertex.getId().get());
      Put put = new Put(rowKey);
      // just need to write the Values
      org.gradoop.model.Vertex v =
        VertexFactory.createDefaultVertexWithID(vertex.getId().get());
      v.addGraph(vertex.getValue().get());
      put = vertexHandler.writeGraphs(put, v);
      writer.write(new ImmutableBytesWritable(rowKey), put);
    }
  }
}
