package org.gradoop.storage.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.GConstants;
import org.gradoop.model.Attributed;
import org.gradoop.model.MultiLabeled;
import org.gradoop.storage.exceptions.UnsupportedTypeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler is used to write labels and properties into HBase tables. This is
 * used by graphs and vertices.
 */
public abstract class BasicHandler implements EntityHandler {
  /**
   * Byte array representation of the labels column family.
   */
  static final byte[] CF_LABELS_BYTES =
    Bytes.toBytes(GConstants.CF_LABELS);

  /**
   * Byte representation of the properties column family.
   */
  static final byte[] CF_PROPERTIES_BYTES =
    Bytes.toBytes(GConstants.CF_PROPERTIES);

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeLabels(final Put put, final MultiLabeled entity) {
    int internalLabelID = 0;
    for (String label : entity.getLabels()) {
      put.add(CF_LABELS_BYTES, Bytes.toBytes(internalLabelID++),
        Bytes.toBytes(label));
    }
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeProperties(final Put put, final Attributed entity) {
    for (String key : entity.getPropertyKeys()) {
      put.add(CF_PROPERTIES_BYTES, Bytes.toBytes(key),
        encodeValueToBytes(entity.getProperty(key)));
    }
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<String> readLabels(final Result res) {
    List<String> labels = new ArrayList<>();
    for (Map.Entry<byte[], byte[]> labelColumn : res
      .getFamilyMap(CF_LABELS_BYTES).entrySet()) {
      labels.add(Bytes.toString(labelColumn.getValue()));
    }
    return labels;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Object> readProperties(final Result res) {
    Map<String, Object> properties = new HashMap<>();
    for (Map.Entry<byte[], byte[]> propertyColumn : res
      .getFamilyMap(CF_PROPERTIES_BYTES)
      .entrySet()) {
      properties
        .put(Bytes.toString(propertyColumn.getKey()), decodeValueFromBytes(
          propertyColumn.getValue()));
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
  protected Iterable<Long> getColumnKeysFromFamiliy(final Result res,
                                                    final byte[] columnFamily) {
    List<Long> keys = Lists.newArrayList();
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
   * Parses an object from a string based on a given type.
   *
   * @param type  object type (must be supported by Gradoop)
   * @param value value as string
   * @return decoded object
   */
  protected Object decodeValueFromString(final byte type, final String value) {
    Object o;
    switch (type) {
      case GConstants.TYPE_BOOLEAN:
        o = Boolean.parseBoolean(value);
        break;
      case GConstants.TYPE_INTEGER:
        o = Integer.parseInt(value);
        break;
      case GConstants.TYPE_LONG:
        o = Long.parseLong(value);
        break;
      case GConstants.TYPE_FLOAT:
        o = Float.parseFloat(value);
        break;
      case GConstants.TYPE_DOUBLE:
        o = Double.parseDouble(value);
        break;
      case GConstants.TYPE_STRING:
        o = value;
        break;
      default:
        throw new UnsupportedTypeException(value.getClass() + " not supported");
    }
    return o;
  }

  /**
   * Encodes a given value to a byte array with integrated type information.
   *
   * @param value value do encode
   * @return encoded value as byte array
   */
  protected byte[] encodeValueToBytes(final Object value)
    throws UnsupportedTypeException {
    Class<?> valueClass = value.getClass();
    byte[] decodedValue;
    if (valueClass.equals(Boolean.class)) {
      decodedValue =
        Bytes.add(new byte[]{GConstants.TYPE_BOOLEAN},
          Bytes.toBytes((Boolean) value));
    } else if (valueClass.equals(Integer.class)) {
      decodedValue =
        Bytes.add(new byte[]{GConstants.TYPE_INTEGER},
          Bytes.toBytes((Integer) value));
    } else if (valueClass.equals(Long.class)) {
      decodedValue = Bytes
        .add(new byte[]{GConstants.TYPE_LONG}, Bytes.toBytes((Long) value));
    } else if (valueClass.equals(Float.class)) {
      decodedValue = Bytes
        .add(new byte[]{GConstants.TYPE_FLOAT}, Bytes.toBytes((Float) value));
    } else if (valueClass.equals(Double.class)) {
      decodedValue = Bytes.add(new byte[]{GConstants.TYPE_DOUBLE},
        Bytes.toBytes((Double) value));
    } else if (valueClass.equals(String.class)) {
      decodedValue = Bytes.add(new byte[]{GConstants.TYPE_STRING},
        Bytes.toBytes((String) value));
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
        case GConstants.TYPE_STRING:
          o = Bytes.toString(value);
          break;
        default:
          throw new UnsupportedTypeException(
            "Type code " + type + " not supported");
      }
    }
    return o;
  }
}