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

/**
 * Created by gomezk on 18.12.14.
 */
public class EPGIdWithValueVertexOutputFormat extends
  HBaseVertexOutputFormat<LongWritable, IntWritable, NullWritable> {

  private static final VertexHandler VERTEX_HANDLER = new EPGVertexHandler();

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWriter<LongWritable, IntWritable, NullWritable>
  createVertexWriter(
    TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    return new EPGIdWithValueVertexWriter(taskAttemptContext);
  }

  public static class EPGIdWithValueVertexWriter extends
    HBaseVertexWriter<LongWritable, IntWritable, NullWritable> {

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
      Vertex<LongWritable, IntWritable, NullWritable> vertex)
      throws IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Mutation> writer = getRecordWriter();
      byte[] rowKey = VERTEX_HANDLER.getRowKey(vertex.getId().get());
      Put put = new Put(rowKey);
      VERTEX_HANDLER.writeProperty(put, EPGLongLongNullVertexInputFormat
        .VALUE_PROPERTY_KEY, vertex.getValue().get());
      writer.write(new ImmutableBytesWritable(rowKey), put);
    }
  }
}
