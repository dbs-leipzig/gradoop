package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by martin on 17.11.14.
 */
public class SimpleHBaseVertexFormatTest extends BspCase {

  private static final HBaseTestingUtility utility = new HBaseTestingUtility();

  private static final long DEFAULT_VERTEX_VALUE = 0L;
  private static final byte[] DEFAULT_VERTEX_VALUE_BYTES =
    Bytes.toBytes(DEFAULT_VERTEX_VALUE);
  private static final long DEFAULT_EDGE_VALUE = 0L;
  private static final byte[] DEFAULT_EDGE_VALUE_BYTES =
    Bytes.toBytes(DEFAULT_EDGE_VALUE);

  public SimpleHBaseVertexFormatTest() {
    super(SimpleHBaseVertexFormatTest.class.getName());
  }

  @Test
  public void vertexInputOutputTest()
    throws Exception {
    utility.startMiniCluster(1).waitForActiveAndReadyMaster();
    utility.startMiniMapReduceCluster();
    HTable testTable = createHBaseTestTable();
    createTestData(testTable);

    Configuration conf = utility.getConfiguration();
    conf.set(TableInputFormat.INPUT_TABLE,
      SimpleHBaseVertexInputFormat.TABLE_NAME);
    conf.set(TableOutputFormat.OUTPUT_TABLE,
      SimpleHBaseVertexInputFormat.TABLE_NAME);

    GiraphJob giraphJob = new GiraphJob(conf, BspCase.getCallingMethodName());
    GiraphConfiguration giraphConfiguration = giraphJob.getConfiguration();
    setupConfiguration(giraphJob);
    giraphConfiguration.setComputationClass(MaximumNeighbourComputation.class);
    giraphConfiguration.setVertexInputFormatClass(SimpleHBaseVertexInputFormat
      .class);
    giraphConfiguration.setVertexOutputFormatClass(SimpleHBaseVertexOutputFormat
      .class);

    assertTrue(giraphJob.run(true));

    validateTestData(testTable);

    testTable.close();
    utility.shutdownMiniMapReduceCluster();
    utility.shutdownMiniCluster();
  }

  private HTable createHBaseTestTable()
    throws IOException {
    Configuration config = utility.getConfiguration();
    HBaseAdmin admin = new HBaseAdmin(config);

    HTableDescriptor verticesTableDescriptor = new HTableDescriptor(TableName
      .valueOf(SimpleHBaseVertexInputFormat.TABLE_NAME));

    if (admin.tableExists(verticesTableDescriptor.getName())) {
      admin.disableTable(verticesTableDescriptor.getName());
      admin.deleteTable(verticesTableDescriptor.getName());
    }

    verticesTableDescriptor.addFamily(new HColumnDescriptor
      (SimpleHBaseVertexInputFormat.CF_VALUE));
    verticesTableDescriptor.addFamily(new HColumnDescriptor
      (SimpleHBaseVertexInputFormat.CF_EDGES));

    admin.createTable(verticesTableDescriptor);
    return new HTable(config, SimpleHBaseVertexInputFormat.TABLE_NAME);
  }

  private void createTestData(HTable table)
    throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    List<Put> puts = Lists.newArrayList();
    puts.add(createTestVertex(0L));
    puts.add(createTestVertex(1L, 0L));
    puts.add(createTestVertex(2L));
    puts.add(createTestVertex(3L));
    puts.add(createTestVertex(4L, 0L));
    puts.add(createTestVertex(5L, 1L, 4L));
    puts.add(createTestVertex(6L, 1L, 5L));
    puts.add(createTestVertex(7L, 3L, 6L));
    puts.add(createTestVertex(8L, 2L, 6L));
    puts.add(createTestVertex(9L, 0L));
    puts.add(createTestVertex(10L, 9L));
    puts.add(createTestVertex(11L, 10L));
    puts.add(createTestVertex(12L, 1L, 10L));
    puts.add(createTestVertex(13L, 2L, 11L));
    puts.add(createTestVertex(14L, 3L, 11L));
    puts.add(createTestVertex(15L, 3L, 12L));
    table.put(puts);
    table.flushCommits();
  }

  private Put createTestVertex(long id, long... neighbours) {
    // vertex id
    Put put = new Put(Bytes.toBytes(id));
    // vertex value
    put.add(SimpleHBaseVertexInputFormat.CF_VALUE_BYTES,
      SimpleHBaseVertexInputFormat.Q_VALUE_BYTES,
      DEFAULT_VERTEX_VALUE_BYTES);
    // outgoing edges
    for (long neighbourId : neighbours) {
      put.add(SimpleHBaseVertexInputFormat.CF_EDGES_BYTES,
        Bytes.toBytes(neighbourId), DEFAULT_EDGE_VALUE_BYTES);
    }
    return put;
  }

  private void validateTestData(HTable table)
    throws IOException {
    validateTextVertex(table, 0L, 9L);
    validateTextVertex(table, 1L, 12L, 0L);
    validateTextVertex(table, 2L, 13L);
    validateTextVertex(table, 3L, 15L);
    validateTextVertex(table, 4L, 5L, 0L);
    validateTextVertex(table, 5L, 6L, 1L, 4L);
    validateTextVertex(table, 6L, 8L, 1L, 5L);
    validateTextVertex(table, 7L, 7L, 3L, 6L);
    validateTextVertex(table, 8L, 8L, 2L, 6L);
    validateTextVertex(table, 9L, 10L, 0L);
    validateTextVertex(table, 10L, 12L, 9L);
    validateTextVertex(table, 11L, 14L, 10L);
    validateTextVertex(table, 12L, 15L, 1L, 10L);
    validateTextVertex(table, 13L, 13L, 2L, 11L);
    validateTextVertex(table, 14L, 14L, 3L, 11L);
    validateTextVertex(table, 15L, 15L, 3L, 12L);
  }

  private void validateTextVertex(HTable table, long vertexId,
                                  long expectedValue,
                                  long... neighbours)
    throws IOException {
    Result result = table.get(new Get(Bytes.toBytes(vertexId)));
    assertFalse(result.isEmpty());
    // value
    byte[] value = result.getValue(SimpleHBaseVertexInputFormat.CF_VALUE_BYTES,
      SimpleHBaseVertexInputFormat.Q_VALUE_BYTES);
    assertEquals(expectedValue, Bytes.toLong(value));
    // edges
    for (long neighbour : neighbours) {
      // target id
      byte[] neighbourBytes = Bytes.toBytes(neighbour);
      assertTrue(result.containsColumn(SimpleHBaseVertexInputFormat
        .CF_EDGES_BYTES, neighbourBytes));
      // edge value
      assertEquals(DEFAULT_EDGE_VALUE,
        Bytes
          .toLong(result.getValue(SimpleHBaseVertexInputFormat.CF_EDGES_BYTES,
            neighbourBytes)));
    }
  }

  /**
   * Example computation that calculates the maximum neighbour vertex id for
   * each vertex.
   */
  public static class MaximumNeighbourComputation extends
    BasicComputation<LongWritable, LongWritable, LongWritable, LongWritable> {

    @Override
    public void compute(Vertex<LongWritable, LongWritable, LongWritable> vertex,
                        Iterable<LongWritable> messages)
      throws IOException {
      if (getSuperstep() == 0) {
        vertex.setValue(vertex.getId());
        sendMessageToAllEdges(vertex, vertex.getValue());
      } else {
        long maxValue = vertex.getValue().get();
        for (LongWritable message : messages) {
          long messageValue = message.get();
          if (messageValue > maxValue) {
            maxValue = messageValue;
          }
        }
        vertex.setValue(new LongWritable(maxValue));
      }
      vertex.voteToHalt();
    }
  }
}
