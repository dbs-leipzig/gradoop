package org.gradoop.algorithms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.gradoop.GConstants;
import org.gradoop.model.Graph;
import org.gradoop.storage.hbase.GraphHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Gradoop sort operator. Sorts HBase rows by column value by creating a
 * secondary index table.
 * <p/>
 * Note: As this is for a prototypical use case, only sorting
 * double values is supported at the moment.
 */
public class Sort {

  /**
   * Configuration key to assign a property key which shall be used for sorting.
   */
  public static final String SORT_PROPERTY_KEY =
    Sort.class.getName() + ".propertykey";

  /**
   * Configuration key to set the sort order.
   */
  public static final String SORT_ASCENDING =
    Sort.class.getName() + ".ascending";

  /**
   * Column family to store value in.
   */
  public static final String COLUMN_FAMILY_NAME = "v";

  /**
   * Column to store value in.
   */
  public static final String COLUMN_NAME = "c";

  /**
   * Sorts the input table by the given column. Creates a new table with the
   * column value and the original row key used as row key.
   */
  public static class SortMapper extends
    TableMapper<ImmutableBytesWritable, Put> {

    /**
     * Byte array for column family identifier
     */
    private static byte[] CF_IDENTIFIER = Bytes.toBytes(COLUMN_FAMILY_NAME);

    /**
     * Byte array for column identifier
     */
    private static byte[] COL_IDENTIFIER = Bytes.toBytes(COLUMN_NAME);

    /**
     * Ascending (true) or descending (false)
     */
    private boolean ascending;

    /**
     * Property to sort
     */
    private String propertyKey;

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

      propertyKey = conf.get(Sort.SORT_PROPERTY_KEY);
      ascending = conf.getBoolean(Sort.SORT_ASCENDING, true);

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
      if (graph.getPropertyCount() > 0) {
        Double v = (Double) graph.getProperty(propertyKey);

        byte[] rowKeyBytes = ascending ? encodeDouble(v, Order.ASCENDING) :
          encodeDouble(v, Order.DESCENDING);

        Put put = new Put(rowKeyBytes);
        put.add(CF_IDENTIFIER, COL_IDENTIFIER, Bytes.toBytes(graph.getId()));

        context.write(new ImmutableBytesWritable(rowKeyBytes), put);
      }
    }

    /**
     * Encodes the given double value to a sortable byte array representation.
     *
     * @param d     double value
     * @param order sorting order
     * @return byte array representing the double value
     */
    private byte[] encodeDouble(Double d, Order order) {
      byte[] a = new byte[Bytes.SIZEOF_DOUBLE + 3];

      PositionedByteRange buf = new SimplePositionedByteRange(a);
      OrderedBytes.encodeNumeric(buf, d, order);

      return buf.getBytes();
    }
  }
}
