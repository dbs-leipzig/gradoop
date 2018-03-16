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
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Wraps the result of a group-by-id-count into a property value
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f1")
public class GroupCountToPropertyValue implements
  MapFunction<Tuple2<GradoopId, Long>, Tuple2<GradoopId, PropertyValue>> {

  @Override
  public Tuple2<GradoopId, PropertyValue> map(
    Tuple2<GradoopId, Long> pair) throws Exception {
    return new Tuple2<>(pair.f0, PropertyValue.create(pair.f1));
  }
}
