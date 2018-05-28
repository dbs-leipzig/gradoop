/**
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
package org.gradoop.common.storage.impl.hbase;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.storage.api.ElementHandler;
import org.gradoop.common.util.HBaseConstants;

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
  static final byte[] CF_META_BYTES = Bytes.toBytes(HBaseConstants.CF_META);

  /**
   * Byte representation of the label column identifier.
   */
  static final byte[] COL_LABEL_BYTES = Bytes.toBytes(HBaseConstants.COL_LABEL);

  /**
   * Byte representation of the properties column family.
   */
  static final byte[] CF_PROPERTIES_BYTES = Bytes.toBytes(HBaseConstants.CF_PROPERTIES);

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getRowKey(final GradoopId elementId) throws IOException {
    return elementId.toByteArray();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getId(final byte[] rowKey) throws IOException {
    if (rowKey == null) {
      throw new IllegalArgumentException("rowKey must not be null");
    }
    return GradoopId.fromByteArray(rowKey);
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
  public Put writeProperty(final Put put, Property property) throws IOException {
    PropertyValue value = property.getValue();
    HBasePropertyValueWrapper wrapper = new HBasePropertyValueWrapper(value);

    put.add(CF_PROPERTIES_BYTES,
      Bytes.toBytes(property.getKey()),
      Writables.getBytes(wrapper));
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeProperties(final Put put, final EPGMElement entity) throws IOException {
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
  public Properties readProperties(final Result res) throws IOException {
    Properties properties = Properties.create();
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
  protected Set<Long> getColumnKeysFromFamily(final Result res, final byte[] columnFamily) {
    Set<Long> keys = Sets.newHashSet();
    for (Map.Entry<byte[], byte[]> column : res.getFamilyMap(columnFamily).entrySet()) {
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
  protected PropertyValue readPropertyValue(final byte[] encValue) throws IOException {
    PropertyValue value = new PropertyValue();
    HBasePropertyValueWrapper wrapper = new HBasePropertyValueWrapper(value);
    Writables.getWritable(encValue, wrapper);
    return value;
  }

  /**
   * Deserializes a gradoop id from HBase row key.
   *
   * @param res HBase row
   * @return gradoop id
   * @throws IOException
   */
  protected GradoopId readId(Result res) throws IOException {
    return GradoopId.fromByteArray(res.getRow());
  }
}
