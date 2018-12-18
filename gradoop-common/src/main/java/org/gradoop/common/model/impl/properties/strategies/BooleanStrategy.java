package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;

public class BooleanStrategy implements PropertyValueStrategy<Boolean> {

  @Override
  public boolean write(Boolean value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
    return true;
  }

  @Override
  public Boolean read(DataInputView inputView, byte typeByte) throws IOException {
    return inputView.readByte() == -1;
  }

  @Override
  public int compare(Boolean value, Object other) {
    if (other.getClass() == Boolean.class) {
      return Boolean.compare(value,(boolean) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof Boolean;
  }

  @Override
  public Class<Boolean> getType() {
    return Boolean.class;
  }

  @Override
  public Boolean get(byte[] bytes) {
    return bytes[1] == -1;
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_BOOLEAN;
  }

  @Override
  public byte[] getRawBytes(Boolean value) {
    byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_BOOLEAN];
    rawBytes[0] = getRawType();
    Bytes.putByte(rawBytes, PropertyValue.OFFSET, (byte) ((boolean) value ? -1 : 0));
    return rawBytes;
  }
}
