/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;
import java.io.IOException;

/**
 * Strategy class for handling {@code PropertyValue} operations when the value is {@code null}.
 */
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
