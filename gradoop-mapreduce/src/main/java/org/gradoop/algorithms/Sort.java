package org.gradoop.algorithms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.model.Graph;
import org.gradoop.storage.hbase.GraphHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Gradoop sort operator. Sorts HBase rows by column value by creating a
 * secondary index table.
 */
public class Sort {

  /**
   * Class logger.
   */
  private static Logger LOG = Logger.getLogger(Sort.class);

  /**
   * Sorts the input table by the given column. Creates a new table with the
   * column value and the original row key used as row key.
   */
  public static class SortMapper extends
    TableMapper<ImmutableBytesWritable, Put> {

    /**
     * Ascending or descending
     */
    private static boolean ASCENDING = true;

    /**
     * Byte array for column family identifier
     */
    private static byte[] CF_NAME = Bytes.toBytes("v");

    /**
     * Byte array for column identifier
     */
    private static byte[] COLUMN_NAME = Bytes.toBytes("k");

    /**
     * Property to sort
     */
    private static String PROPERTY_KEY =
      SelectAndAggregate.DEFAULT_AGGREGATE_RESULT_PROPERTY_KEY;

    /**
     * Reads/writes graphs from/to HBase rows.
     */
    private GraphHandler graphHandler;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setup(Context context) throws IOException,
      InterruptedException {
      Configuration conf = context.getConfiguration();

      Class<? extends GraphHandler> handlerClass = conf
        .getClass(GConstants.GRAPH_HANDLER_CLASS,
          GConstants.DEFAULT_GRAPH_HANDLER, GraphHandler.class);

      try {
        this.graphHandler = handlerClass.getConstructor().newInstance();
      } catch (NoSuchMethodException | InstantiationException |
        IllegalAccessException | InvocationTargetException e) {
        e.printStackTrace();
      }
    }

    /**
     * {@inheritDoc}
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
      Context context) throws IOException, InterruptedException {
      Graph graph = graphHandler.readGraph(value);
      LOG.info("=== processing graph " + graph.getID() + " with property " +
        "count: " + graph.getPropertyCount());
      if (graph.getPropertyCount() > 0) {
        Double v = (Double) graph.getProperty(PROPERTY_KEY);
        LOG.info("=== value to sort: " + v.toString());
        if (!ASCENDING) {
          v = Double.MAX_VALUE - v;
        }

        byte[] rowKeyBytes = Bytes.toBytes(v);
        Put put = new Put(rowKeyBytes);
        put.add(CF_NAME, COLUMN_NAME, Bytes.toBytes(graph.getID()));

        context.write(new ImmutableBytesWritable(rowKeyBytes), put);
      }
    }
  }
}
