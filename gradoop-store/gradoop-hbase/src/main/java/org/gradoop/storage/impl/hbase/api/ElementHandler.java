/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Handles writing and reading label and properties of an EPGM entity,
 * i.e., vertex, edge and graph data.
 */
public interface ElementHandler extends Serializable {
  /**
   * Creates a globally unique row key based on the given gradoop id. The
   * created row key is used to persist the entity in the graph store.
   *
   * @param gradoopId the gradoop id used to create row key from
   * @return persistent entity identifier
   */
  byte[] getRowKey(@Nonnull final GradoopId gradoopId) throws IOException;

  /**
   * Returns a list of all possible row keys for the given gradoop id according to
   * the spreading byte.
   *
   * @param elementId the gradoop id of the element
   * @return a list of all possible row keys
   */
  List<byte[]> getPossibleRowKeys(@Nonnull final GradoopId elementId);

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
   * Creates an identifier from a given row {@link Result}.
   *
   * @param res row result
   * @return entity identifier
   */
  GradoopId readId(@Nonnull final Result res) throws IOException;

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

  /**
   * Enable/Disable the usage of pre-splitting regions at the moment of table creation.
   * If the HBase table size grows, it should be created with pre-split regions in order to avoid
   * region hotspots. If certain region servers are stressed by very intensive write/read
   * operations, HBase may drop that region server because the Zookeeper connection will timeout.
   *
   * Note that this flag has no effect if the tables already exist.
   *
   * @param usePreSplitRegions flag to decide if the pre-split of regions should be used
   */
  void setPreSplitRegions(boolean usePreSplitRegions);

  /**
   * Enable/Disable the usage of a spreading byte as prefix of each HBase row key. This affects
   * reading and writing from/to HBase.
   *
   * Records in HBase are sorted lexicographically by the row key. This allows fast access to
   * an individual record by its key and fast fetching of a range of data given start and stop keys.
   * But writing records with such naive keys will cause hotspotting because of how HBase writes
   * data to its Regions. With this option you can disable this hotspotting.
   *
   * @param useSpreadingByte flag to decide if the spreading byte should be used
   */
  void setSpreadingByteUsage(boolean useSpreadingByte);

  /**
   * Indicates whether the regions in HBase tables are pre-split or not.
   *
   * @return true, if a pre-split of regions is used
   */
  boolean isPreSplitRegions();

  /**
   * Indicates whether the row key is salted with a spreading byte as prefix or not.
   *
   * @return true, if a spreading byte is used as row key prefix
   */
  boolean isSpreadingByteUsed();
}
