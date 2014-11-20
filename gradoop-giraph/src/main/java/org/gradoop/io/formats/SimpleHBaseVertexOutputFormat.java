package org.gradoop.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by martin on 17.11.14.
 */
public class SimpleHBaseVertexOutputFormat extends
  HBaseVertexOutputFormat<LongWritable, LongWritable, LongWritable> {

  @Override
  public VertexWriter<LongWritable, LongWritable, LongWritable>
  createVertexWriter(
    TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new EPGHBaseVertexWriter(context);
  }

  public static class EPGHBaseVertexWriter extends
    HBaseVertexWriter<LongWritable, LongWritable, LongWritable> {

    public EPGHBaseVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      super(context);
    }

    @Override
    public void writeVertex(
      Vertex<LongWritable, LongWritable, LongWritable> vertex)
      throws IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Mutation> writer = getRecordWriter();
      byte[] rowKey = Bytes.toBytes(vertex.getId().get());
      // vertex id
      Put put = new Put(rowKey);
      // vertex value
      put.add(SimpleHBaseVertexInputFormat.CF_VALUE_BYTES,
        SimpleHBaseVertexInputFormat.Q_VALUE_BYTES, Bytes.toBytes(vertex
          .getValue().get()));
      // edges
      for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
        put.add(SimpleHBaseVertexInputFormat.CF_EDGES_BYTES,
          Bytes.toBytes(edge.getTargetVertexId().get()),
          Bytes.toBytes(edge.getValue().get()));
      }
      writer.write(new ImmutableBytesWritable(rowKey), put);
    }
  }
}
