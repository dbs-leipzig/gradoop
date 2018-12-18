package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;

public class ShortStrategy implements PropertyValueStrategy<Short> {

  @Override
  public boolean write(Short value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
    return true;
  }

  @Override
  public Short read(DataInputView inputView, byte typeByte) throws IOException {
    int length = Bytes.SIZEOF_SHORT;
    byte[] rawBytes = new byte[length];

    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }
    return Bytes.toShort(rawBytes);
  }

  @Override
  public int compare(Short value, Object other) {
    if (other instanceof Number) {
      Number num = (Number) other;
      return PropertyValueStrategyUtils.compareNumerical(value, num);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof Short;
  }

  @Override
  public Class<Short> getType() {
    return Short.class;
  }

  @Override
  public Short get(byte[] bytes) {
    return Bytes.toShort(bytes, PropertyValue.OFFSET);
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_SHORT;
  }

  @Override
  public byte[] getRawBytes(Short value) {
    byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_SHORT];
    rawBytes[0] = getRawType();
    Bytes.putShort(rawBytes, PropertyValue.OFFSET, value);
    return rawBytes;
  }
}
