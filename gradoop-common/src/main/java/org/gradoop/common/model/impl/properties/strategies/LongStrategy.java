package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;

public class LongStrategy implements PropertyValueStrategy<Long> {

  @Override
  public boolean write(Long value, DataOutputView outputView) throws IOException {
    byte[] rawBytes = getRawBytes(value);
    outputView.write(rawBytes);
    return true;
  }

  @Override
  public Long read(DataInputView inputView, byte typeByte) throws IOException {
    int length = Bytes.SIZEOF_LONG;
    byte[] rawBytes = new byte[length];
    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }
    return Bytes.toLong(rawBytes);
  }

  @Override
  public int compare(Long value, Object other) {
    if (other instanceof Number) {
      Number num = (Number) other;
      return PropertyValueStrategyUtils.compareNumerical(value, num);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof Long;
  }

  @Override
  public Class<Long> getType() {
    return Long.class;
  }

  @Override
  public Long get(byte[] bytes) {
    return Bytes.toLong(bytes, PropertyValue.OFFSET);
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_LONG;
  }

  @Override
  public byte[] getRawBytes(Long value) {
    byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_LONG];
    rawBytes[0] = getRawType();
    Bytes.putLong(rawBytes, PropertyValue.OFFSET, value);
    return rawBytes;
  }
}
