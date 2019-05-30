/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.Type;

import java.io.IOException;
import java.util.Arrays;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code GradoopIdStrategy}.
 */
public class GradoopIdStrategy extends AbstractFixSizedPropertyValueStrategy<GradoopId> {

  @Override
  public GradoopId read(DataInputView inputView, byte typeByte) throws IOException {
    int length = GradoopId.ID_SIZE;
    byte[] rawBytes = new byte[length];

    for (int i = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }

    return GradoopId.fromByteArray(rawBytes);
  }

  @Override
  public int compare(GradoopId value, Object other) {
    if (other instanceof GradoopId) {
      return value.compareTo((GradoopId) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof GradoopId;
  }

  @Override
  public Class<GradoopId> getType() {
    return GradoopId.class;
  }

  @Override
  public GradoopId get(byte[] bytes) {
    return GradoopId.fromByteArray(
      Arrays.copyOfRange(
        bytes, PropertyValue.OFFSET, GradoopId.ID_SIZE + PropertyValue.OFFSET
      ));
  }

  @Override
  public byte getRawType() {
    return Type.GRADOOP_ID.getTypeByte();
  }

  @Override
  public byte[] getRawBytes(GradoopId value) {
    byte[] valueBytes = value.toByteArray();
    byte[] rawBytes = new byte[PropertyValue.OFFSET + GradoopId.ID_SIZE];
    rawBytes[0] = getRawType();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
