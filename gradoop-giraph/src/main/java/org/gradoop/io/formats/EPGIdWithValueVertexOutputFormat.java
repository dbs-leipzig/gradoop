package org.gradoop.io.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Used to write the result of a graph computation into HBase. The graph is
 * based on the EPG model.
 */
public class EPGIdWithValueVertexOutputFormat extends
  HBaseVertexOutputFormat<LongWritable, LongWritable, NullWritable> {


  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWriter<LongWritable, LongWritable, NullWritable>
  createVertexWriter(
    TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    return new EPGIdWithValueVertexWriter(taskAttemptContext);
  }

  public static class EPGIdWithValueVertexWriter extends
    HBaseVertexWriter<LongWritable, LongWritable, NullWritable> {

    /**
     * Sets up base table output format and creates a record writer.
     *
     * @param context task attempt context
     */
    public EPGIdWithValueVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      super(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertex(
      Vertex<LongWritable, LongWritable, NullWritable> vertex)
      throws IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Mutation> writer = getRecordWriter();
      VertexHandler vertexHandler = getVertexHandler();
      byte[] rowKey = vertexHandler.getRowKey(vertex.getId().get());
      Put put = new Put(rowKey);
      vertexHandler.writeProperty(put, EPGLongLongNullVertexInputFormat
        .VALUE_PROPERTY_KEY, vertex.getValue().get());

      writer.write(new ImmutableBytesWritable(rowKey), put);
    }
  }
}
