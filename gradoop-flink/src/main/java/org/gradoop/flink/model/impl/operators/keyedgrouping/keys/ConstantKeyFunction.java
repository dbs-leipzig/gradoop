/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.keyedgrouping.keys;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;

/**
 * A grouping key function that returns the same value for every object.<p>
 * <i>Note:</i> This returns {@link Boolean#TRUE} for every element.
 *
 * @param <T> The type of the elements to group.
 */
public class ConstantKeyFunction<T> implements KeyFunctionWithDefaultValue<T, Boolean> {

  @Override
  public Boolean getDefaultKey() {
    return Boolean.TRUE;
  }

  @Override
  public Boolean getKey(T element) {
    return Boolean.TRUE;
  }

  @Override
  public TypeInformation<Boolean> getType() {
    return BasicTypeInfo.BOOLEAN_TYPE_INFO;
  }
}
