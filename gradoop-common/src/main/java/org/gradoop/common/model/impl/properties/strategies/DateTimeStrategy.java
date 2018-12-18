package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.DateTimeSerializer;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;

public class DateTimeStrategy implements PropertyValueStrategy<LocalDateTime> {

  @Override
  public boolean write(LocalDateTime value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
    return true;
  }

  @Override
  public LocalDateTime read(DataInputView inputView, byte typeByte) throws IOException {
    int length = DateTimeSerializer.SIZEOF_DATETIME;
    byte[] rawBytes = new byte[length];

    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }

    return DateTimeSerializer.deserializeDateTime(rawBytes);
  }

  @Override
  public int compare(LocalDateTime value, Object other) {
    if (other instanceof LocalDateTime) {
      return value.compareTo((LocalDateTime) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof LocalDateTime;
  }

  @Override
  public Class<LocalDateTime> getType() {
    return LocalDateTime.class;
  }

  @Override
  public LocalDateTime get(byte[] bytes) {
    return DateTimeSerializer.deserializeDateTime(
      Arrays.copyOfRange(
        bytes, PropertyValue.OFFSET, DateTimeSerializer.SIZEOF_DATETIME + PropertyValue.OFFSET
      ));
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_DATETIME;
  }

  @Override
  public byte[] getRawBytes(LocalDateTime value) {
    byte[] valueBytes = DateTimeSerializer.serializeDateTime(value);
    byte[] rawBytes = new byte[PropertyValue.OFFSET + DateTimeSerializer.SIZEOF_DATETIME];
    rawBytes[0] = getRawType();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
