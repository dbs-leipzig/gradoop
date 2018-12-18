package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;

public class StringStrategy implements PropertyValueStrategy<String> {

  @Override
  public boolean write(String value, DataOutputView outputView) throws IOException {
    byte[] rawBytes = getRawBytes(value);
    byte type = rawBytes[0];

    if (rawBytes.length > PropertyValue.LARGE_PROPERTY_THRESHOLD) {
      type |= PropertyValue.FLAG_LARGE;
    }
    outputView.writeByte(type);
    // Write length as an int if the "large" flag is set.
    if ((type & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
      outputView.writeInt(rawBytes.length - PropertyValue.OFFSET);
    } else {
      outputView.writeShort(rawBytes.length - PropertyValue.OFFSET);
    }

    outputView.write(rawBytes, PropertyValue.OFFSET, rawBytes.length - PropertyValue.OFFSET);
    return true;
  }

  @Override
  public String read(DataInputView inputView, byte typeByte) throws IOException {
    int length;
    // read length
    if ((typeByte & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
      length = inputView.readInt();
    } else {
      length = inputView.readShort();
    }
    byte[] rawBytes = new byte[length];
    for (int i = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }
    return Bytes.toString(rawBytes);
  }

  @Override
  public int compare(String value, Object other) {
    if (other instanceof String) {
      return value.compareTo((String) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof String;
  }

  @Override
  public Class<String> getType() {
    return String.class;
  }

  @Override
  public String get(byte[] bytes) {
    return Bytes.toString(bytes, PropertyValue.OFFSET, bytes.length - PropertyValue.OFFSET);
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_STRING;
  }

  @Override
  public byte[] getRawBytes(String value) {
    byte[] valueBytes = Bytes.toBytes(value);
    byte[] rawBytes = new byte[PropertyValue.OFFSET + valueBytes.length];
    rawBytes[0] = getRawType();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
