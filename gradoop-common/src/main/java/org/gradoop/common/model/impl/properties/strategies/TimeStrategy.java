package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.DateTimeSerializer;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;
import java.time.LocalTime;
import java.util.Arrays;

public class TimeStrategy implements PropertyValueStrategy<LocalTime> {

  @Override
  public boolean write(LocalTime value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
    return true;
  }

  @Override
  public LocalTime read(DataInputView inputView, byte typeByte) throws IOException {
    int length = DateTimeSerializer.SIZEOF_TIME;
    byte[] rawBytes = new byte[length];

    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }

    return DateTimeSerializer.deserializeTime(rawBytes);
  }

  @Override
  public int compare(LocalTime value, Object other) {
    if (other instanceof LocalTime) {
      return value.compareTo((LocalTime) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof LocalTime;
  }

  @Override
  public Class<LocalTime> getType() {
    return LocalTime.class;
  }

  @Override
  public LocalTime get(byte[] bytes) {
    return DateTimeSerializer.deserializeTime(
      Arrays.copyOfRange(
        bytes, PropertyValue.OFFSET, DateTimeSerializer.SIZEOF_TIME + PropertyValue.OFFSET
      ));
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_TIME;
  }

  @Override
  public byte[] getRawBytes(LocalTime value) {
    byte[] valueBytes = DateTimeSerializer.serializeTime(value);
    byte[] rawBytes = new byte[PropertyValue.OFFSET + DateTimeSerializer.SIZEOF_TIME];
    rawBytes[0] = getRawType();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
