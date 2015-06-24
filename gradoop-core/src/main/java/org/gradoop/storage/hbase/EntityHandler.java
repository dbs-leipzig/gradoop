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

package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.Attributed;
import org.gradoop.model.Labeled;

import java.io.IOException;
import java.util.Map;

/**
 * Handles writing and reading label and properties of an EPGM entity, normally
 * vertices and graphs.
 */
public interface EntityHandler {
  /**
   * Adds the labels to the given HBase put and returns it.
   *
   * @param put    put to write label to
   * @param entity entity to use the label from
   * @return put with label
   */
  Put writeLabel(final Put put, final Labeled entity);

  /**
   * Adds the given key-value-pair to the put and returns it.
   *
   * @param put   put to write property to
   * @param key   property key
   * @param value property value
   * @return put with property
   */
  Put writeProperty(final Put put, final String key, final Object value);

  /**
   * Adds all properties to the given HBase put and returns it.
   *
   * @param put    put to write properties to
   * @param entity entity to use properties from
   * @return put with properties
   */
  Put writeProperties(final Put put, final Attributed entity);

  /**
   * Reads the label from the given HBase row result.
   *
   * @param res HBase row result
   * @return label contained in the row
   */
  String readLabel(final Result res);

  /**
   * Reads all properties from the given HBase row result.
   *
   * @param res HBase row result
   * @return all properties contained in the row
   */
  Map<String, Object> readProperties(final Result res);

  /**
   * Creates table based on the given table descriptor.
   *
   * @param admin           HBase admin
   * @param tableDescriptor description of the table used by that specific
   *                        handler
   * @throws java.io.IOException
   */
  void createTable(final HBaseAdmin admin,
    final HTableDescriptor tableDescriptor) throws IOException;
}
