package org.gradoop.io.formats;

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
import org.apache.log4j.Logger;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by gomezk on 18.12.14.
 */
public class EPGLongLongNullVertexInputFormat extends
  HBaseVertexInputFormat<LongWritable, LongWritable, NullWritable> {

  private static Logger LOG = Logger.getLogger
    (EPGLongLongNullVertexInputFormat.class);

  /**
   * Property key to identify the corresponding vertex value.
   */
  public static final String VALUE_PROPERTY_KEY = "v";
  /**
   * Vertex handler to read vertices from HBase.
   */
  private static final VertexHandler VERTEX_HANDLER = new EPGVertexHandler();

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexReader<LongWritable, LongWritable,
    NullWritable> createVertexReader(
    InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException {
    return new EPGLongIntNullVertexReader(inputSplit, taskAttemptContext);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void checkInputSpecs(Configuration entries) {
  }

  public static class EPGLongIntNullVertexReader extends
    HBaseVertexReader<LongWritable, LongWritable, NullWritable> {
    /**
     * Sets the base TableInputFormat and creates a record reader.
     *
     * @param split   InputSplit
     * @param context Context
     * @throws java.io.IOException
     */
    public EPGLongIntNullVertexReader(InputSplit split,
                                      TaskAttemptContext context)
      throws IOException {
      super(split, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nextVertex()
      throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex<LongWritable, LongWritable, NullWritable> getCurrentVertex()
      throws IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();

      LOG.info("=== reading vertex");

      LongWritable vertexID = new LongWritable(VERTEX_HANDLER.getVertexID(
        row.getRow()));

      LOG.info("=== vertex id: " + vertexID);

      Map<String, Object> props = VERTEX_HANDLER.readProperties(row);

      LOG.info("=== properties");
      for (Map.Entry<String, Object> e : props.entrySet()) {
        LOG.info(e.getKey() + " => " + e.getValue());
      }

      LongWritable vertexValue = new LongWritable((long) props.get
        (VALUE_PROPERTY_KEY));

      LOG.info("=== vertex value: " + vertexValue);

      // read outgoing edges
      LOG.info("=== outgoing edges");
      List<Edge<LongWritable, NullWritable>> edges = Lists.newArrayList();
      for (org.gradoop.model.Edge e : VERTEX_HANDLER.readOutgoingEdges(row)) {
        LOG.info("=== otherID: " + e.getOtherID());
        edges.add(EdgeFactory.create(new LongWritable(e.getOtherID())));
      }

      Vertex<LongWritable, LongWritable, NullWritable> vertex = getConf()
        .createVertex();

      vertex.initialize(vertexID, vertexValue, edges);

      LOG.info("=== EO reading vertex");
      return vertex;
    }
  }
}
