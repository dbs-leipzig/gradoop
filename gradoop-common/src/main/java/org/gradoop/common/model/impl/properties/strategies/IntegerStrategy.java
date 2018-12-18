package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;

public class IntegerStrategy implements PropertyValueStrategy<Integer> {

  @Override
  public boolean write(Integer value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
    return true;
  }

  @Override
  public Integer read(DataInputView inputView, byte typeByte) throws IOException {
    int length = Bytes.SIZEOF_INT;
    byte[] rawBytes = new byte[length];
    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }
    return Bytes.toInt(rawBytes);
  }

  @Override
  public int compare(Integer value, Object other) {
    if (other instanceof Number) {
      Number num = (Number) other;
      return PropertyValueStrategyUtils.compareNumerical(value, num);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof Integer;
  }

  @Override
  public Class<Integer> getType() {
    return Integer.class;
  }

  @Override
  public Integer get(byte[] bytes) {
    return Bytes.toInt(bytes, PropertyValue.OFFSET);
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_INTEGER;
  }

  @Override
  public byte[] getRawBytes(Integer value) {
    byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_INT];
    rawBytes[0] = getRawType();
    Bytes.putInt(rawBytes, PropertyValue.OFFSET, value);
    return rawBytes;
  }
}
