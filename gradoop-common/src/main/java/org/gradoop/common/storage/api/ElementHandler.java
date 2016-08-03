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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.common.storage.api;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyList;

import java.io.IOException;
import java.io.Serializable;

/**
 * Handles writing and reading label and properties of an EPGM entity,
 * i.e., vertex, edge and graph data.
 */
public interface ElementHandler extends Serializable {
  /**
   * Creates a globally unique row key based on the given entity data. The
   * created row key is used to persist the entity in the graph store.
   *
   * @param entityData used to create row key from (must not be {@code
   *                   null}).
   * @return persistent entity identifier
   */
  byte[] getRowKey(final GradoopId entityData) throws IOException;

  /**
   * Creates an identifier from a given row key.
   *
   * @param rowKey row key from the graph store (must not be {@code null})
   * @return entity identifier
   */
  GradoopId getId(final byte[] rowKey) throws IOException;

  /**
   * Adds the labels to the given {@link Put} and returns it.
   *
   * @param put    put to write label to
   * @param entity entity to use the label from
   * @return put with label
   */
  Put writeLabel(final Put put, final EPGMElement entity);

  /**
   * Adds the given property to the {@link Put} and returns it.
   *
   * @param put   put to write property to
   * @param property property
   * @return put with property
   */
  Put writeProperty(final Put put, final Property property) throws
    IOException;

  /**
   * Adds all properties of the given element to the given {@link Put} and
   * returns it.
   *
   * @param put    put to write properties to
   * @param entity entity to use properties from
   * @return put with properties
   */
  Put writeProperties(final Put put, final EPGMElement entity) throws
    IOException;

  /**
   * Reads the label from the given row {@link Result}.
   *
   * @param res row result
   * @return label contained in the row
   */
  String readLabel(final Result res);

  /**
   * Reads all properties from the given row {@link Result}..
   *
   * @param res row result
   * @return all properties contained in the row
   */
  PropertyList readProperties(final Result res) throws IOException;

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
