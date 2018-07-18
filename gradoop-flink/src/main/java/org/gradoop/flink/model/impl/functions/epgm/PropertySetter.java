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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Sets a property value of an EPGM element.
 *
 * @param <EL> EPGM element type
 * @param <T>  property type
 */
@FunctionAnnotation.ForwardedFieldsFirst("id")
public class PropertySetter<EL extends Element, T>
  implements JoinFunction<EL, Tuple2<GradoopId, T>, EL> {

  /**
   * Property key
   */
  private final String propertyKey;

  /**
   * Creates a new instance.
   *
   * @param propertyKey property key to set value
   */
  public PropertySetter(final String propertyKey) {
    this.propertyKey = checkNotNull(propertyKey);
  }

  @Override
  public EL join(EL element, Tuple2<GradoopId, T> propertyTuple) throws
    Exception {
    element.setProperty(propertyKey, propertyTuple.f1);
    return element;
  }
}
