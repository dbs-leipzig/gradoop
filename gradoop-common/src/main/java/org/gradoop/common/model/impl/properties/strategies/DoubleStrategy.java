package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;

public class DoubleStrategy implements PropertyValueStrategy<Double> {

  @Override
  public boolean write(Double value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
    return true;
  }

  @Override
  public Double read(DataInputView inputView, byte typeByte) throws IOException {
    int length = Bytes.SIZEOF_DOUBLE;
    byte[] rawBytes = new byte[length];
    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }
    return Bytes.toDouble(rawBytes);
  }

  @Override
  public int compare(Double value, Object other) {
    if (other instanceof Number) {
      Number num = (Number) other;
      return PropertyValueStrategyUtils.compareNumerical(value, num);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof Double;
  }

  @Override
  public Class<Double> getType() {
    return Double.class;
  }

  @Override
  public Double get(byte[] bytes) {
    return Bytes.toDouble(bytes, PropertyValue.OFFSET);
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_DOUBLE;
  }

  @Override
  public byte[] getRawBytes(Double value) {
    byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_DOUBLE];
    rawBytes[0] = getRawType();
    Bytes.putDouble(rawBytes, PropertyValue.OFFSET, value);
    return rawBytes;
  }
}
