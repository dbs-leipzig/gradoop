/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

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
   * Default constructor
   */
  public ValueWritable() {
  }

  /**
   * Creates new Value writable from the given value.
   *
   * @param value writable value
   */
  public ValueWritable(Writable value) {
    this.set(value);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  protected Class<? extends Writable>[] getTypes() {
    return CLASSES;
  }
}
