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

package org.gradoop.common.storage.impl.hbase;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.storage.api.ElementHandler;
import org.gradoop.common.util.GConstants;
import org.gradoop.common.model.impl.properties.PropertyList;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Handler is used to write label and properties into HBase tables. This is
 * used by graphs and vertices.
 */
public abstract class HBaseElementHandler implements ElementHandler {
  /**
   * Byte representation of the meta column family.
   */
  static final byte[] CF_META_BYTES = Bytes.toBytes(GConstants.CF_META);

  /**
   * Byte representation of the label column identifier.
   */
  static final byte[] COL_LABEL_BYTES = Bytes.toBytes(GConstants.COL_LABEL);

  /**
   * Byte representation of the properties column family.
   */
  static final byte[] CF_PROPERTIES_BYTES =
    Bytes.toBytes(GConstants.CF_PROPERTIES);

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getRowKey(final GradoopId elementId) throws IOException {
    if (elementId == null) {
      throw new IllegalArgumentException("elementId must not be null");
    }
    return Writables.getBytes(elementId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getId(final byte[] rowKey) throws IOException {
    if (rowKey == null) {
      throw new IllegalArgumentException("rowKey must not be null");
    }
    GradoopId id = new GradoopId();
    Writables.getWritable(rowKey, id);
    return id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeLabel(final Put put, final EPGMElement entity) {
    return (entity.getLabel() == null) ? put :
      put.add(CF_META_BYTES, COL_LABEL_BYTES, Bytes.toBytes(entity.getLabel()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeProperty(final Put put, Property property)
      throws IOException {
    put.add(CF_PROPERTIES_BYTES,
      Bytes.toBytes(property.getKey()),
      Writables.getBytes(property.getValue()));
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeProperties(final Put put, final EPGMElement entity)
      throws IOException {
    if (entity.getPropertyCount() > 0) {
      for (Property property : entity.getProperties()) {
        writeProperty(put, property);
      }
    }
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String readLabel(final Result res) {
    return Bytes.toString(res.getValue(CF_META_BYTES, COL_LABEL_BYTES));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PropertyList readProperties(final Result res) throws IOException {
    PropertyList properties = PropertyList.create();
    Map<byte[], byte[]> familyMap = res.getFamilyMap(CF_PROPERTIES_BYTES);
    for (Map.Entry<byte[], byte[]> propertyColumn : familyMap.entrySet()) {
      properties.set(
        readPropertyKey(propertyColumn.getKey()),
        readPropertyValue(propertyColumn.getValue()));
    }
    return properties;
  }

  /**
   * Returns all column keys inside a column family.
   *
   * @param res          HBase row result
   * @param columnFamily column family to get keys from
   * @return all keys inside column family.
   */
  protected Set<Long> getColumnKeysFromFamily(final Result res,
    final byte[] columnFamily) {
    Set<Long> keys = Sets.newHashSet();
    for (Map.Entry<byte[], byte[]> column : res.getFamilyMap(columnFamily)
      .entrySet()) {
      keys.add(Bytes.toLong(column.getKey()));
    }
    return keys;
  }

  /**
   * Reads the property key from the given byte array.
   *
   * @param encKey encoded property key
   * @return property key
   */
  protected String readPropertyKey(final byte[] encKey) {
    return Bytes.toString(encKey);
  }

  /**
   * Decodes a value from a given byte array.
   *
   * @param encValue encoded property value
   * @return property value
   */
  protected PropertyValue readPropertyValue(final byte[] encValue) throws
    IOException {
    PropertyValue p = new PropertyValue();
    Writables.getWritable(encValue, p);
    return p;
  }

  /**
   * Deserializes a gradoop id from HBase row key.
   *
   * @param res HBase row
   * @return gradoop id
   * @throws IOException
   */
  protected GradoopId readId(Result res) throws IOException {
    GradoopId id = new GradoopId();
    Writables.getWritable(res.getRow(), id);

    return id;
  }
}
