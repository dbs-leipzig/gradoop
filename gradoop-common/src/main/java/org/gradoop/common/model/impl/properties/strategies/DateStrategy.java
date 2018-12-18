package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.DateTimeSerializer;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;

public class DateStrategy implements PropertyValueStrategy<LocalDate> {

  @Override
  public boolean write(LocalDate value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
    return true;
  }

  @Override
  public LocalDate read(DataInputView inputView, byte typeByte) throws IOException {
    int length = DateTimeSerializer.SIZEOF_DATE;
    byte[] rawBytes = new byte[length];

    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }

    return DateTimeSerializer.deserializeDate(rawBytes);
  }

  @Override
  public int compare(LocalDate value, Object other) {
    if (other instanceof LocalDate) {
      return value.compareTo((LocalDate) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof LocalDate;
  }

  @Override
  public Class<LocalDate> getType() {
    return LocalDate.class;
  }

  @Override
  public LocalDate get(byte[] bytes) {
    return DateTimeSerializer.deserializeDate(
      Arrays.copyOfRange(
        bytes, PropertyValue.OFFSET, DateTimeSerializer.SIZEOF_DATE + PropertyValue.OFFSET
      ));
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_DATE;
  }

  @Override
  public byte[] getRawBytes(LocalDate value) {
    byte[] valueBytes = DateTimeSerializer.serializeDate(value);
    byte[] rawBytes = new byte[PropertyValue.OFFSET + DateTimeSerializer.SIZEOF_DATE];
    rawBytes[0] = getRawType();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
