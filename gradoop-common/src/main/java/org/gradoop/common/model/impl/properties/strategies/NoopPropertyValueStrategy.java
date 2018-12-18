package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;

public class NoopPropertyValueStrategy implements PropertyValueStrategy {
  @Override
  public boolean write(Object value, DataOutputView outputView) throws IOException {
    outputView.write(new byte[]{0});
    return true;
  }

  @Override
  public Object read(DataInputView inputView, byte typeByte) throws IOException {
    return null;
  }

  @Override
  public int compare(Object value, Object other) {
    return 0;
  }

  @Override
  public boolean is(Object value) {
    return false;
  }

  @Override
  public Class<?> getType() {
    return null;
  }

  @Override
  public Object get(byte[] bytes) {
    return null;
  }

  @Override
  public Byte getRawType() {
    return null;
  }

  @Override
  public byte[] getRawBytes(Object value) {
    return null;
  }
}
