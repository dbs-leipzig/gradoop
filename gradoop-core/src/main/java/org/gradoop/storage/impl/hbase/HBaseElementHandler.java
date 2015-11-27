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

package org.gradoop.storage.impl.hbase;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.api.EPGMProperties;
import org.gradoop.model.impl.properties.Properties;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.storage.api.ElementHandler;
import org.gradoop.storage.exceptions.UnsupportedTypeException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.math.BigDecimal;
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
  public Put writeProperty(final Put put, final String key,
    final Object value) {
    put.add(CF_PROPERTIES_BYTES, Bytes.toBytes(key), encodeValueToBytes(value));
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeProperties(final Put put, final EPGMElement entity) {
    if (entity.getPropertyCount() > 0) {
      for (String key : entity.getPropertyKeys()) {
        writeProperty(put, key, entity.getProperty(key));
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
  public EPGMProperties readProperties(final Result res) {

    EPGMProperties properties = new Properties();

    for (Map.Entry<byte[], byte[]> propertyColumn : res
      .getFamilyMap(CF_PROPERTIES_BYTES).entrySet()) {
      properties.set(
        Bytes.toString(propertyColumn.getKey()),
        decodeValueFromBytes(propertyColumn.getValue())
      );
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
   * Returns the type of a given object if it's supported.
   *
   * @param o object
   * @return type of object
   */
  protected byte getType(final Object o) {
    Class<?> valueClass = o.getClass();
    byte type;
    if (valueClass.equals(Boolean.class)) {
      type = GConstants.TYPE_BOOLEAN;
    } else if (valueClass.equals(Integer.class)) {
      type = GConstants.TYPE_INTEGER;
    } else if (valueClass.equals(Long.class)) {
      type = GConstants.TYPE_LONG;
    } else if (valueClass.equals(Long.class)) {
      type = GConstants.TYPE_FLOAT;
    } else if (valueClass.equals(Double.class)) {
      type = GConstants.TYPE_DOUBLE;
    } else if (valueClass.equals(String.class)) {
      type = GConstants.TYPE_STRING;
    } else {
      throw new UnsupportedTypeException(valueClass + " not supported");
    }
    return type;
  }

  /**
   * Encodes a given value to a byte array with integrated type information.
   *
   * @param value value do encode
   * @return encoded value as byte array
   */
  protected byte[] encodeValueToBytes(final Object value) throws
    UnsupportedTypeException {
    Class<?> valueClass = value.getClass();
    byte[] decodedValue;
    if (valueClass.equals(Boolean.class)) {
      decodedValue = Bytes.add(new byte[]{GConstants.TYPE_BOOLEAN},
        Bytes.toBytes((Boolean) value));
    } else if (valueClass.equals(Integer.class)) {
      decodedValue = Bytes.add(new byte[]{GConstants.TYPE_INTEGER},
        Bytes.toBytes((Integer) value));
    } else if (valueClass.equals(Long.class)) {
      decodedValue = Bytes
        .add(new byte[]{GConstants.TYPE_LONG}, Bytes.toBytes((Long) value));
    } else if (valueClass.equals(Float.class)) {
      decodedValue = Bytes
        .add(new byte[]{GConstants.TYPE_FLOAT}, Bytes.toBytes((Float) value));
    } else if (valueClass.equals(Double.class)) {
      decodedValue = Bytes
        .add(new byte[]{GConstants.TYPE_DOUBLE}, Bytes.toBytes((Double) value));
    } else if (valueClass.equals(String.class)) {
      decodedValue = Bytes.add(new byte[] {GConstants.TYPE_STRING},
        Bytes.toBytes((String) value));
    } else if (valueClass.equals(BigDecimal.class)) {
        decodedValue = Bytes.add(new byte[]{GConstants.TYPE_DECIMAL},
          Bytes.toBytes((BigDecimal) value));
    } else if (valueClass.equals(BigDecimal.class)) {
      decodedValue = Bytes.add(new byte[]{GConstants.TYPE_DATE},
        Bytes.toBytes(((DateTime) value).getMillis()));
    } else {
      throw new UnsupportedTypeException(valueClass + " not supported");
    }
    return decodedValue;
  }

  /**
   * Decodes a value from a given byte array.
   *
   * @param encValue encoded value with type information
   * @return decoded value
   */
  protected Object decodeValueFromBytes(final byte[] encValue) {
    Object o = null;
    if (encValue.length > 0) {
      byte type = encValue[0];
      byte[] value = Bytes.tail(encValue, encValue.length - 1);
      switch (type) {
      case GConstants.TYPE_BOOLEAN:
        o = Bytes.toBoolean(value);
        break;
      case GConstants.TYPE_INTEGER:
        o = Bytes.toInt(value);
        break;
      case GConstants.TYPE_LONG:
        o = Bytes.toLong(value);
        break;
      case GConstants.TYPE_FLOAT:
        o = Bytes.toFloat(value);
        break;
      case GConstants.TYPE_DOUBLE:
        o = Bytes.toDouble(value);
        break;
      case GConstants.TYPE_DECIMAL:
        o = Bytes.toBigDecimal(value);
        break;
      case GConstants.TYPE_STRING:
        o = Bytes.toString(value);
        break;
      case GConstants.TYPE_DATE:
        o = new DateTime(Bytes.toLong(value));
        break;
      default:
        throw new UnsupportedTypeException(
          "Type code " + type + " not supported");
      }
    }
    return o;
  }

  /**
   * deserializes a gradoop id from hbase key
   * @param res hbase row
   * @return gradoop od
   * @throws IOException
   */
  protected GradoopId readId(Result res) throws IOException {
    GradoopId id = new GradoopId();
    Writables.getWritable(res.getRow(), id);

    return id;
  }
}
