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
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

/**
 * Used to write resulting vertices of BTG Computation to HBase.
 */
public class BTGHBaseVertexOutputFormat extends
  HBaseVertexOutputFormat<LongWritable, BTGVertexValue, NullWritable> {

  /**
   * Vertex handler to write vertices to HBase.
   */
  private static final VertexHandler VERTEX_HANDLER = new EPGVertexHandler();

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
    public void writeVertex(
      Vertex<LongWritable, BTGVertexValue, NullWritable> vertex) throws
      IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Mutation> writer = getRecordWriter();
      byte[] rowKey = VERTEX_HANDLER.getRowKey(vertex.getId().get());
      Put put = new Put(rowKey);
      // just need to write the graphs
      put = VERTEX_HANDLER.writeGraphs(put, vertex.getValue());

      writer.write(new ImmutableBytesWritable(rowKey), put);
    }
  }
}
