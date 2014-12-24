package org.gradoop.io.formats;

import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.GConstants;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Base class that wraps an HBase TableInputFormat and underlying Scan object to
 * help instantiate vertices from an HBase table. All the static
 * TableInputFormat properties necessary to configure an HBase job are
 * available.
 * <p/>
 * For example, setting conf.set(TableInputFormat.INPUT_TABLE, "in_table"); from
 * the job setup routine will properly delegate to the TableInputFormat
 * instance. The Configurable interface prevents specific wrapper methods from
 * having to be called.
 * <p/>
 * Works with {@link HBaseVertexOutputFormat}
 * <p/>
 * Note: Class is taken from giraph-hbase and adapted to HBase 0.98.7
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public abstract class HBaseVertexInputFormat<I extends WritableComparable, V
  extends Writable, E extends Writable> extends
  VertexInputFormat<I, V, E> {

  /**
   * delegate HBase table input format
   */
  protected static final TableInputFormat BASE_FORMAT = new TableInputFormat();

  /**
   * {@inheritDoc}
   */
  @Override
  public void checkInputSpecs(Configuration conf) {
  }

  /**
   * Takes an instance of RecordReader that supports HBase row-key, result
   * records.  Subclasses can focus on vertex instantiation details without
   * worrying about connection semantics. Subclasses are expected to implement
   * nextVertex() and getCurrentVertex()
   *
   * @param <I> Vertex index value
   * @param <V> Vertex value
   * @param <E> Edge value
   */
  public abstract static class HBaseVertexReader<I extends
    WritableComparable, V extends Writable, E extends Writable> extends
    VertexReader<I, V, E> {

    /**
     * Reader instance
     */
    private final RecordReader<ImmutableBytesWritable, Result> reader;

    /**
     * Vertex handler to read vertices from HBase.
     */
    private VertexHandler vertexHandler;

    /**
     * Context passed to initialize
     */
    private TaskAttemptContext context;

    /**
     * Sets the base TableInputFormat and creates a record reader.
     *
     * @param split   InputSplit
     * @param context Context
     * @throws java.io.IOException
     */
    public HBaseVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
      BASE_FORMAT.setConf(context.getConfiguration());
      this.reader = BASE_FORMAT.createRecordReader(split, context);
    }

    /**
     * Initialize the HBase Reader and the Vertex handler.
     *
     * @param inputSplit Input split to be used for reading vertices.
     * @param context    Context from the task.
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
      reader.initialize(inputSplit, context);
      this.context = context;

      Class<? extends VertexHandler> handlerClass = getConf()
        .getClass(GConstants.VERTEX_HANDLER_CLASS,
          GConstants.DEFAULT_VERTEX_HANDLER, VertexHandler.class);
      try {
        this.vertexHandler = handlerClass.getConstructor().newInstance();
      } catch (NoSuchMethodException | InstantiationException |
        IllegalAccessException | InvocationTargetException e) {
        e.printStackTrace();
      }
    }

    /**
     * close
     *
     * @throws IOException
     */
    public void close() throws IOException {
      reader.close();
    }

    /**
     * getProgress
     *
     * @return progress
     * @throws IOException
     * @throws InterruptedException
     */
    public float getProgress() throws IOException, InterruptedException {
      return reader.getProgress();
    }

    /**
     * getRecordReader
     *
     * @return Record reader to be used for reading.
     */
    protected RecordReader<ImmutableBytesWritable, Result> getRecordReader() {
      return reader;
    }

    /**
     * getContext
     *
     * @return Context passed to initialize.
     */
    protected TaskAttemptContext getContext() {
      return context;
    }

    /**
     * Returns the vertex handler.
     *
     * @return Vertex handler to be used for reading vertices.
     */
    protected VertexHandler getVertexHandler() {
      return vertexHandler;
    }

  }

  @Override
  public List<InputSplit> getSplits(JobContext context,
    int minSplitCountHint) throws IOException, InterruptedException {
    BASE_FORMAT.setConf(getConf());
    return BASE_FORMAT.getSplits(context);
  }
}

