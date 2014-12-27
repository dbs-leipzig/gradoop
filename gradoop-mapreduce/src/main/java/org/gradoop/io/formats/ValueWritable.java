package org.gradoop.io.formats;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Used to transfer values of EPG supported types between map and reduce phase.
 */
public class ValueWritable extends GenericWritable {

  /**
   * Writable implementations that can be stored as value.
   */
  private static Class<? extends Writable>[] CLASSES = null;

  static {
    CLASSES = (Class<? extends Writable>[]) new Class[]{DoubleWritable.class,
                                                        IntWritable.class,
                                                        FloatWritable.class,
                                                        ByteWritable.class,
                                                        Text.class};
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Class<? extends Writable>[] getTypes() {
    return CLASSES;
  }
}
