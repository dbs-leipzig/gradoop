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
package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;

/**
 * Describes a vertex aggregate function as input for the
 * {@link Aggregation} operator.
 */
public interface VertexAggregateFunction extends AggregateFunction {

  /**
   * Describes the increment of a vertex that should be added to the aggregate.
   *
   * @param vertex vertex
   *
   * @return increment, may be NULL, which is handled in the operator
   */
  PropertyValue getVertexIncrement(Vertex vertex);
}
