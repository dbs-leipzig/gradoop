package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
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
 * Created by martin on 20.11.14.
 */
public class EPGHBaseVertexInputFormat extends HBaseVertexInputFormat<
  EPGVertexIdentifierWritable,
  EPGMultiLabeledAttributedWritable,
  EPGMultiLabeledAttributedWritable> {

  private static final VertexHandler VERTEX_HANDLER = new EPGVertexHandler();

  @Override
  public VertexReader<EPGVertexIdentifierWritable, EPGMultiLabeledAttributedWritable,
    EPGMultiLabeledAttributedWritable> createVertexReader(
    InputSplit split, TaskAttemptContext context)
    throws IOException {
    return new EPGHBaseVertexReader(split, context);
  }

  @Override
  public void checkInputSpecs(Configuration conf) {

  }

  public static class EPGHBaseVertexReader extends
    HBaseVertexReader<EPGVertexIdentifierWritable, EPGMultiLabeledAttributedWritable,
      EPGMultiLabeledAttributedWritable> {

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
    public Vertex<EPGVertexIdentifierWritable, EPGMultiLabeledAttributedWritable,
      EPGMultiLabeledAttributedWritable> getCurrentVertex()
      throws IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();

      Vertex<EPGVertexIdentifierWritable, EPGMultiLabeledAttributedWritable,
        EPGMultiLabeledAttributedWritable> vertex = getConf().createVertex();

      // vertexID
      EPGVertexIdentifierWritable vertexID = new EPGVertexIdentifierWritable(Bytes
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

      // TODO: initialize edges using EDGE_HANDLER
//      // outgoing edges
//      Map<String, Map<String, Object>> outEdges = VERTEX_HANDLER
//        .readOutgoingEdges(row);
//
//      // incoming edges
//      Map<String, Map<String, Object>> inEdges = VERTEX_HANDLER
//        .readIncomingEdges(row);

      List<Edge<EPGVertexIdentifierWritable, EPGMultiLabeledAttributedWritable>> edges =
        Lists.newArrayList();

      vertex.initialize(vertexID, vertexValue, edges);

      return vertex;
    }
  }
}
