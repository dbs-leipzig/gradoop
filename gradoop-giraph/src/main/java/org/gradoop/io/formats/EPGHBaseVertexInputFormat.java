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
    EPGLabeledAttributedWritable, EPGEdgeValueWritable> {

  /**
   * Configuration key used to specify if edges are seen directed (false) or
   * undirected (true) (i.e., for giraph computations)
   */
  public static final String READ_INCOMING_EDGES =
    "gradoop.io.epgvertexinputformat.readincomingedges";

  /**
   * Default value false, only needed in computation like connected
   * components in giraph
   */
  private static final boolean READ_INCOMING_EDGES_DEFAULT_VALUE = false;

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexReader<EPGVertexIdentifierWritable,
    EPGLabeledAttributedWritable, EPGEdgeValueWritable> createVertexReader(
    InputSplit split, TaskAttemptContext context) throws IOException {
    return new EPGHBaseVertexReader(split, context);
  }

  /**
   * Reads an EPG vertex from a given HBase row result.
   */
  public static class EPGHBaseVertexReader extends
    HBaseVertexReader<EPGVertexIdentifierWritable,
      EPGLabeledAttributedWritable, EPGEdgeValueWritable> {

    /**
     * If true, read incoming edges to simulate undirected edges (i.e., for
     * giraph computations)
     */
    private boolean readIncomingEdges;

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
    public void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
      super.initialize(inputSplit, context);

      readIncomingEdges = getConf()
        .getBoolean(READ_INCOMING_EDGES, READ_INCOMING_EDGES_DEFAULT_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex<EPGVertexIdentifierWritable, EPGLabeledAttributedWritable,
      EPGEdgeValueWritable> getCurrentVertex() throws
      IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();
      VertexHandler vertexHandler = getVertexHandler();

      Vertex<EPGVertexIdentifierWritable, EPGLabeledAttributedWritable,
        EPGEdgeValueWritable>
        vertex = getConf().createVertex();

      // vertexID
      EPGVertexIdentifierWritable vertexID = new EPGVertexIdentifierWritable(
        vertexHandler.getVertexID(row.getRow()));

      // label
      String label = vertexHandler.readLabel(row);
      // properties
      Map<String, Object> properties = vertexHandler.readProperties(row);
      // graphs
      Iterable<Long> graphs = vertexHandler.readGraphs(row);
      // create vertex value
      EPGVertexValueWritable vertexValue =
        new EPGVertexValueWritable(label, properties, graphs);

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
      if (readIncomingEdges) {
        for (org.gradoop.model.Edge edge : vertexHandler
          .readIncomingEdges(row)) {
          EPGVertexIdentifierWritable edgeTarget =
            new EPGVertexIdentifierWritable(edge.getOtherID());
          EPGEdgeValueWritable edgeValue = new EPGEdgeValueWritable(edge);
          edges.add(EdgeFactory.create(edgeTarget, edgeValue));
        }
      }

      vertex.initialize(vertexID, vertexValue, edges);

      return vertex;
    }
  }
}
