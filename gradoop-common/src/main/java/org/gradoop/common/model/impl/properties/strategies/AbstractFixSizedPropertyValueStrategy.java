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

import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.io.IOException;

/**
 * Abstract class that provides generic methods for {@code PropertyValueStrategy} classes that
 * handle data types with a fixed size.
 *
 * @param <T> Type with a fixed length.
 */
public abstract class AbstractFixSizedPropertyValueStrategy<T> implements PropertyValueStrategy<T> {

  @Override
  public void write(T value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
  }
}
