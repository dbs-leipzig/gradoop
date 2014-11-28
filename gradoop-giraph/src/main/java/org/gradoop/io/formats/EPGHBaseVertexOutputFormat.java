package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.List;

/**
 * Used to write the result of a graph computation into HBase. The graph is
 * based on the EPG model.
 */
public class EPGHBaseVertexOutputFormat extends HBaseVertexOutputFormat<
  EPGVertexIdentifierWritable, EPGVertexValueWritable,
  EPGEdgeValueWritable> {

  private static final VertexHandler VERTEX_HANDLER = new EPGVertexHandler();

  private static final Logger LOG = Logger.getLogger
    (EPGHBaseVertexOutputFormat.class);

  @Override
  public VertexWriter<EPGVertexIdentifierWritable, EPGVertexValueWritable,
    EPGEdgeValueWritable> createVertexWriter(
    TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new EPGHBaseVertexWriter(context);
  }

  public static class EPGHBaseVertexWriter extends HBaseVertexWriter<
    EPGVertexIdentifierWritable, EPGVertexValueWritable, EPGEdgeValueWritable> {

    /**
     * Sets up base table output format and creates a record writer.
     *
     * @param context task attempt context
     */
    public EPGHBaseVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      super(context);
    }

    @Override
    public void writeVertex(
      Vertex<EPGVertexIdentifierWritable, EPGVertexValueWritable,
        EPGEdgeValueWritable> vertex)
      throws IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Mutation> writer = getRecordWriter();
      byte[] rowKey = Bytes.toBytes(vertex.getId().getID());
      Put put = new Put(rowKey);
      // labels
      VERTEX_HANDLER.writeLabels(put, vertex.getValue());
      // properties
      VERTEX_HANDLER.writeProperties(put, vertex.getValue());
      // graphs
      VERTEX_HANDLER.writeGraphs(put, vertex.getValue());
      // outgoing edges
      List<org.gradoop.model.Edge> edges = Lists.newArrayListWithCapacity
        (vertex.getNumEdges());
      for (Edge<EPGVertexIdentifierWritable, EPGEdgeValueWritable> edge :
        vertex.getEdges()) {
        edges.add(edge.getValue());
      }
      VERTEX_HANDLER.writeOutgoingEdges(put, edges);

      writer.write(new ImmutableBytesWritable(rowKey), put);
    }
  }
}
