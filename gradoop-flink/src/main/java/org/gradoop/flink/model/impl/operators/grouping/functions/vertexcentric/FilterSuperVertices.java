/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.grouping.tuples.vertexcentric.VertexGroupItem;

/**
 * Filter those tuples which are used to create new super vertices.
 */
@FunctionAnnotation.ReadFields("f5")
public class FilterSuperVertices implements FilterFunction<VertexGroupItem> {

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(VertexGroupItem vertexGroupItem) throws Exception {
    return vertexGroupItem.isSuperVertex();
  }
}
