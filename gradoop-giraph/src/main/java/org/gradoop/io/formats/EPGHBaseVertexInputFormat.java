package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Used to read a EPG based graph from HBase into Giraph.
 */
public class EPGHBaseVertexInputFormat extends HBaseVertexInputFormat<
  EPGVertexIdentifierWritable, EPGMultiLabeledAttributedWritable,
  EPGEdgeValueWritable> {

  /**
   * Vertex handler to read vertices from HBase.
   */
  private static final VertexHandler VERTEX_HANDLER = new EPGVertexHandler();

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexReader<EPGVertexIdentifierWritable,
    EPGMultiLabeledAttributedWritable,
    EPGEdgeValueWritable> createVertexReader(
    InputSplit split, TaskAttemptContext context)
    throws IOException {
    return new EPGHBaseVertexReader(split, context);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void checkInputSpecs(Configuration conf) {

  }

  public static class EPGHBaseVertexReader extends
    HBaseVertexReader<EPGVertexIdentifierWritable,
      EPGMultiLabeledAttributedWritable,
      EPGEdgeValueWritable> {

    /**
     * Sets the base TableInputFormat and creates a record reader.
     *
     * @param split   InputSplit
     * @param context Context
     * @throws java.io.IOException
     */
    public EPGHBaseVertexReader(InputSplit split,
                                TaskAttemptContext context)
      throws IOException {
      super(split, context);
    }

    @Override
    public boolean nextVertex()
      throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    @Override
    public Vertex<EPGVertexIdentifierWritable,
      EPGMultiLabeledAttributedWritable,
      EPGEdgeValueWritable> getCurrentVertex()
      throws IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();

      Vertex<EPGVertexIdentifierWritable, EPGMultiLabeledAttributedWritable,
        EPGEdgeValueWritable> vertex = getConf().createVertex();

      // vertexID
      EPGVertexIdentifierWritable vertexID =
        new EPGVertexIdentifierWritable(Bytes
          .toLong(row.getRow()));

      // labels
      Iterable<String> labels = VERTEX_HANDLER.readLabels(row);
      // properties
      Map<String, Object> properties = VERTEX_HANDLER.readProperties(row);
      // graphs
      Iterable<Long> graphs = VERTEX_HANDLER.readGraphs(row);
      // create vertex value
      EPGVertexValueWritable vertexValue = new
        EPGVertexValueWritable(labels, properties, graphs);

      List<Edge<EPGVertexIdentifierWritable, EPGEdgeValueWritable>>
        edges = Lists.newArrayList();
      // outgoing edges
      for (org.gradoop.model.Edge edge : VERTEX_HANDLER
        .readOutgoingEdges(row)) {
        EPGVertexIdentifierWritable edgeTarget = new
          EPGVertexIdentifierWritable(edge.getOtherID());
        EPGEdgeValueWritable edgeValue = new EPGEdgeValueWritable(edge);
        edges.add(EdgeFactory.create(edgeTarget, edgeValue));
      }
      // incoming edges
//      for (org.gradoop.model.Edge edge : VERTEX_HANDLER
//        .readIncomingEdges(row)) {
//        EPGVertexIdentifierWritable edgeTarget = new
//          EPGVertexIdentifierWritable(edge.getOtherID());
//        EPGEdgeValueWritable edgeValue = new EPGEdgeValueWritable(
//          edge.getLabel());
//        edges.add(EdgeFactory.create(edgeTarget, edgeValue));
//      }

      vertex.initialize(vertexID, vertexValue, edges);

      return vertex;
    }
  }
}
