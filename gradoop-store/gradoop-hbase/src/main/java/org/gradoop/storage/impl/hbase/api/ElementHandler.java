/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.storage.impl.hbase.api;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.Properties;

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
  Put writeLabel(
    final Put put,
    final EPGMElement entity
  );

  /**
   * Adds the given property to the {@link Put} and returns it.
   *
   * @param put   put to write property to
   * @param property property
   * @return put with property
   */
  Put writeProperty(final Put put, final Property property);

  /**
   * Adds all properties of the given element to the given {@link Put} and
   * returns it.
   *
   * @param put    put to write properties to
   * @param entity entity to use properties from
   * @return put with properties
   */
  Put writeProperties(final Put put, final EPGMElement entity);

  /**
   * Reads the label from the given row {@link Result}.
   *
   * @param res row result
   * @return label contained in the row
   */
  String readLabel(final Result res);

  /**
   * Reads all properties from the given row {@link Result}.
   *
   * @param res row result
   * @return all properties contained in the row
   */
  Properties readProperties(final Result res);

  /**
   * Creates table based on the given table descriptor.
   *
   * @param admin           HBase admin
   * @param tableDescriptor description of the table used by that specific handler
   * @throws IOException on failure
   */
  void createTable(final Admin admin, final HTableDescriptor tableDescriptor) throws IOException;
}
