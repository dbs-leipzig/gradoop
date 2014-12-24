package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Used to read a EPG based graph from HBase into Giraph.
 */
public class EPGHBaseVertexInputFormat extends
  HBaseVertexInputFormat<EPGVertexIdentifierWritable,
    EPGMultiLabeledAttributedWritable, EPGEdgeValueWritable> {

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexReader<EPGVertexIdentifierWritable,
    EPGMultiLabeledAttributedWritable, EPGEdgeValueWritable> createVertexReader(
    InputSplit split, TaskAttemptContext context) throws IOException {
    return new EPGHBaseVertexReader(split, context);
  }

  /**
   * Reads an EPG vertex from a given HBase row result.
   */
  public static class EPGHBaseVertexReader extends
    HBaseVertexReader<EPGVertexIdentifierWritable,
      EPGMultiLabeledAttributedWritable, EPGEdgeValueWritable> {

    /**
     * Sets the base TableInputFormat and creates a record reader.
     *
     * @param split   InputSplit
     * @param context Context
     * @throws java.io.IOException
     */
    public EPGHBaseVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
      super(split, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex<EPGVertexIdentifierWritable,
      EPGMultiLabeledAttributedWritable, EPGEdgeValueWritable>
    getCurrentVertex() throws
      IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();
      VertexHandler vertexHandler = getVertexHandler();

      Vertex<EPGVertexIdentifierWritable, EPGMultiLabeledAttributedWritable,
        EPGEdgeValueWritable>
        vertex = getConf().createVertex();

      // vertexID
      EPGVertexIdentifierWritable vertexID = new EPGVertexIdentifierWritable(
        vertexHandler.getVertexID(row.getRow()));

      // labels
      Iterable<String> labels = vertexHandler.readLabels(row);
      // properties
      Map<String, Object> properties = vertexHandler.readProperties(row);
      // graphs
      Iterable<Long> graphs = vertexHandler.readGraphs(row);
      // create vertex value
      EPGVertexValueWritable vertexValue =
        new EPGVertexValueWritable(labels, properties, graphs);

      List<Edge<EPGVertexIdentifierWritable, EPGEdgeValueWritable>> edges =
        Lists.newArrayList();
      // outgoing edges
      for (org.gradoop.model.Edge edge : vertexHandler.readOutgoingEdges(row)) {
        EPGVertexIdentifierWritable edgeTarget =
          new EPGVertexIdentifierWritable(edge.getOtherID());
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
