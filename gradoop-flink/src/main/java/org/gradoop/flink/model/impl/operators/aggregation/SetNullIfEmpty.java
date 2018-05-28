/**
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
package org.gradoop.flink.model.impl.operators.aggregation;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Iterator;

/**
 * NULL, aggregateValue => aggregateValue
 * OR
 * NULL => NULL
 */
public class SetNullIfEmpty
  implements GroupReduceFunction<PropertyValue, PropertyValue> {

  @Override
  public void reduce(Iterable<PropertyValue> values,
    Collector<PropertyValue> out) throws Exception {

    Iterator<PropertyValue> iterator = values.iterator();

    PropertyValue value = iterator.next();

    if (iterator.hasNext() && value.equals(PropertyValue.NULL_VALUE)) {
      value = iterator.next();
    }

    out.collect(value);
  }
}
