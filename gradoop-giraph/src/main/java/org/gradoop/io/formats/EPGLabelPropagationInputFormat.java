package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.List;

/**
 * Used to read a EPG based graph from HBase into Giraph.
 */
public class EPGLabelPropagationInputFormat extends
  HBaseVertexInputFormat<LongWritable, LongWritable, NullWritable> {


  /**
   * {@inheritDoc}
   */
  @Override
  public VertexReader<LongWritable, LongWritable, NullWritable>
  createVertexReader(
    InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws
    IOException {
    return new EPGLongLongNullVertexReader(inputSplit, taskAttemptContext);
  }

  /**
   * Reads a single vertex from HBase.
   */
  public static class EPGLongLongNullVertexReader extends
    HBaseVertexReader<LongWritable, LongWritable, NullWritable> {

    /**
     * Sets the base TableInputFormat and creates a record reader.
     *
     * @param split   InputSplit
     * @param context Context
     */
    public EPGLongLongNullVertexReader(InputSplit split,
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
    public Vertex<LongWritable, LongWritable, NullWritable> getCurrentVertex
    () throws
      IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();
      VertexHandler vertexHandler = getVertexHandler();


      LongWritable vertexID =
        new LongWritable(vertexHandler.getVertexID(row.getRow()));


      List<Edge<LongWritable, NullWritable>> edges = Lists.newArrayList();


      // read outgoing edges
      for (org.gradoop.model.Edge e : vertexHandler.readOutgoingEdges(row)) {
        edges.add(EdgeFactory.create(new LongWritable(e.getOtherID())));
      }

      for (org.gradoop.model.Edge e : vertexHandler.readIncomingEdges(row)) {
        edges.add(EdgeFactory.create(new LongWritable(e.getOtherID())));
      }

      Vertex<LongWritable, LongWritable, NullWritable> vertex =
        getConf().createVertex();

      vertex.initialize(vertexID, vertexID, edges);
      return vertex;


    }
  }
}
