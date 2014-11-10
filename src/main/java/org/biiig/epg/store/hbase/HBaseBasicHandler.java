package org.biiig.epg.store.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.biiig.epg.model.Attributed;
import org.biiig.epg.model.Labeled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by s1ck on 11/10/14.
 */
public abstract class HBaseBasicHandler implements HBaseEntityHandler {
  protected static final byte[] CF_LABELS_BYTES = Bytes.toBytes(HBaseGraphStore.CF_LABELS);
  protected static final byte[] CF_PROPERTIES_BYTES = Bytes.toBytes(HBaseGraphStore.CF_PROPERTIES);

  private static final byte TYPE_BOOLEAN = 0x00;
  private static final byte TYPE_INTEGER = 0x01;
  private static final byte TYPE_LONG = 0x02;
  private static final byte TYPE_FLOAT = 0x03;
  private static final byte TYPE_DOUBLE = 0x04;
  private static final byte TYPE_STRING = 0x05;

  @Override public Put writeLabels(Put put, Labeled entity) {
    int internalLabelID = 0;
    for (String label : entity.getLabels()) {
      put.add(CF_LABELS_BYTES, Bytes.toBytes(internalLabelID++), Bytes.toBytes(label));
    }
    return put;
  }

  @Override public Put writeProperties(Put put, Attributed entity) {
    for (String key : entity.getPropertyKeys()) {
      put.add(CF_PROPERTIES_BYTES, Bytes.toBytes(key),
          encodeValue(entity.getProperty(key)));
    }
    return put;
  }

  @Override public Iterable<String> readLabels(Result res) {
    List<String> labels = new ArrayList<>();
    for (Map.Entry<byte[], byte[]> labelColumn : res.getFamilyMap(CF_LABELS_BYTES).entrySet()) {
      labels.add(Bytes.toString(labelColumn.getValue()));
    }
    return labels;
  }

  @Override public Map<String, Object> readProperties(Result res) {
    Map<String, Object> properties = new HashMap<>();
    for (Map.Entry<byte[], byte[]> propertyColumn : res.getFamilyMap(CF_PROPERTIES_BYTES)
        .entrySet()) {
      properties
          .put(Bytes.toString(propertyColumn.getKey()), decodeValue(propertyColumn.getValue()));
    }
    return properties;
  }

  protected Iterable<Long> getColumnKeysFromFamiliy(Result res, byte[] columnFamily) {
    List<Long> keys = Lists.newArrayList();
    for (Map.Entry<byte[], byte[]> column : res.getFamilyMap(columnFamily).entrySet()) {
      keys.add(Bytes.toLong(column.getKey()));
    }
    return keys;
  }

  private byte[] encodeValue(Object value) {
    Class<?> valueClass = value.getClass();
    byte[] decodedValue;
    if (valueClass.equals(Boolean.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_BOOLEAN }, Bytes.toBytes((Boolean) value));
    } else if (valueClass.equals(Integer.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_INTEGER }, Bytes.toBytes((Integer) value));
    } else if (valueClass.equals(Long.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_LONG }, Bytes.toBytes((Long) value));
    } else if (valueClass.equals(Float.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_FLOAT }, Bytes.toBytes((Float) value));
    } else if (valueClass.equals(Double.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_DOUBLE }, Bytes.toBytes((Double) value));
    } else if (valueClass.equals(String.class)) {
      decodedValue = Bytes.add(new byte[] { TYPE_STRING }, Bytes.toBytes((String) value));
    } else {
      throw new IllegalArgumentException(
          valueClass + " not supported by graph store " + HBaseGraphStore.class);
    }
    return decodedValue;
  }

  private Object decodeValue(byte[] encValue) {
    Object o = null;
    if (encValue.length > 0) {
      byte type = encValue[0];
      byte[] value = Bytes.tail(encValue, encValue.length - 1);
      switch (type) {
      case TYPE_BOOLEAN:
        o = Bytes.toBoolean(value);
        break;
      case TYPE_INTEGER:
        o = Bytes.toInt(value);
        break;
      case TYPE_LONG:
        o = Bytes.toLong(value);
        break;
      case TYPE_FLOAT:
        o = Bytes.toFloat(value);
        break;
      case TYPE_DOUBLE:
        o = Bytes.toDouble(value);
        break;
      case TYPE_STRING:
        o = Bytes.toString(value);
        break;
      }
    }
    return o;
  }
}