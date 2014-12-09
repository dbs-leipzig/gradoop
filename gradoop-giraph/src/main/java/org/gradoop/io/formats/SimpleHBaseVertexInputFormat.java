package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by martin on 17.11.14.
 */
public class SimpleHBaseVertexInputFormat extends
  HBaseVertexInputFormat<LongWritable, LongWritable, LongWritable> {

  static final String CF_VALUE = "value";
  static final String CF_EDGES = "edges";
  static final String Q_VALUE = CF_VALUE;

  static final byte[] CF_VALUE_BYTES = Bytes.toBytes(CF_VALUE);
  static final byte[] CF_EDGES_BYTES = Bytes.toBytes(CF_EDGES);
  static final byte[] Q_VALUE_BYTES = Bytes.toBytes(Q_VALUE);

  @Override
  public VertexReader<LongWritable, LongWritable,
    LongWritable> createVertexReader(InputSplit split,
                                     TaskAttemptContext context)
    throws IOException {
    return new SimpleHBaseVertexReader(split, context);
  }

  @Override
  public void checkInputSpecs(Configuration conf) {
  }

  /**
   * Uses the HBase RecordReader to create vertices from HBase rows.
   */
  public static class SimpleHBaseVertexReader extends
    HBaseVertexReader<LongWritable, LongWritable, LongWritable> {

    public SimpleHBaseVertexReader(InputSplit split,
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
    public Vertex<LongWritable, LongWritable, LongWritable> getCurrentVertex()
      throws IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();

      Vertex<LongWritable, LongWritable, LongWritable> vertex = getConf()
        .createVertex();

      // vertex id
      LongWritable vertexID = new LongWritable(Bytes.toLong(row.getRow()));

      // vertex value
      LongWritable vertexValue = new LongWritable(Bytes.toLong(row.getValue
        (CF_VALUE_BYTES, Q_VALUE_BYTES)));

      // edges
      List<Edge<LongWritable, LongWritable>> edges = Lists.newLinkedList();

      for (Map.Entry<byte[], byte[]> edgeColumn : row.getFamilyMap
        (CF_EDGES_BYTES).entrySet()) {
        // target vertex
        LongWritable edgeTarget = new LongWritable(Bytes.toLong(edgeColumn
          .getKey()));
        // edge value
        LongWritable edgeValue = new LongWritable(Bytes.toLong(edgeColumn
          .getValue()));
        edges.add(EdgeFactory.create(edgeTarget, edgeValue));
      }
      vertex.initialize(vertexID, vertexValue, edges);
      return vertex;
    }
  }
}
