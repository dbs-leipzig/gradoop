package org.gradoop.biiig.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.io.formats.HBaseVertexInputFormat;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.List;

/**
 * Used to read vertices for BTG Computation from HBase.
 */
public class BTGHBaseVertexInputFormat extends
  HBaseVertexInputFormat<LongWritable, BTGVertexValue, NullWritable> {

  /**
   * Vertex handler to read vertices from HBase.
   */
  private static final VertexHandler VERTEX_HANDLER = new EPGVertexHandler();

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexReader<LongWritable, BTGVertexValue, NullWritable>
  createVertexReader(
    InputSplit split, TaskAttemptContext context) throws IOException {
    return new BTGHBaseVertexReader(split, context);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void checkInputSpecs(Configuration conf) {

  }

  /**
   * Reads a single giraph vertex for BTG computation from a HBase row result.
   */
  public static class BTGHBaseVertexReader extends
    HBaseVertexReader<LongWritable, BTGVertexValue, NullWritable> {

    /**
     * Sets the base TableInputFormat and creates a record reader.
     *
     * @param split   InputSplit
     * @param context Context
     * @throws java.io.IOException
     */
    public BTGHBaseVertexReader(InputSplit split,
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
    public Vertex<LongWritable, BTGVertexValue, NullWritable>
    getCurrentVertex() throws
      IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();

      Vertex<LongWritable, BTGVertexValue, NullWritable> vertex =
        getConf().createVertex();

      // vertexID
      LongWritable vertexID =
        new LongWritable(VERTEX_HANDLER.getVertexID(row.getRow()));

      // vertex type is stored the first label of the vertex
      Iterable<String> vertexLabels = VERTEX_HANDLER.readLabels(row);
      Integer firstLabel = Integer.valueOf(vertexLabels.iterator().next());
      BTGVertexType vertexType = BTGVertexType.values()[firstLabel];

      // initial vertex value is the vertex id
      Double vertexValue = new Double(String.valueOf(vertexID));

      // btgIDs are the graphs this vertex belongs to
      List<Long> btgIDs = Lists.newArrayList(VERTEX_HANDLER.readGraphs(row));

      BTGVertexValue btgVertexValue =
        new BTGVertexValue(vertexType, vertexValue, btgIDs);

      // read outgoing edges
      List<Edge<LongWritable, NullWritable>> edges = Lists.newArrayList();
      for (org.gradoop.model.Edge e : VERTEX_HANDLER.readOutgoingEdges(row)) {
        edges.add(EdgeFactory.create(new LongWritable(e.getOtherID())));
      }
      // read incoming edges
      for (org.gradoop.model.Edge e : VERTEX_HANDLER.readIncomingEdges(row)) {
        edges.add(EdgeFactory.create(new LongWritable(e.getOtherID())));
      }

      vertex.initialize(vertexID, btgVertexValue, edges);

      return vertex;
    }
  }
}
