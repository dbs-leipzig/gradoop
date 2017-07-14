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
package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;

/**
 * Filter to get items which either represent new, concatenated super vertices or those which
 * represent vertices which stay unchanged.
 */
@FunctionAnnotation.ReadFields("f0")
public class FilterSuperVertexGroupItem implements FilterFunction<SuperVertexGroupItem> {

  /**
   * True, to get concatenated super vertex group items, false to get items where the vertex
   * stays unchanged.
   */
  private boolean filterConcatenatedSuperVertices;

  /**
   * Constructor.
   *
   * @param filterConcatenatedSuperVertices true, for concatenated items, false for unchanged
   *                                        vertices
   */
  public FilterSuperVertexGroupItem(boolean filterConcatenatedSuperVertices) {
    this.filterConcatenatedSuperVertices = filterConcatenatedSuperVertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(SuperVertexGroupItem superVertexGroupItem) throws Exception {
    if (filterConcatenatedSuperVertices) {
      return superVertexGroupItem.getVertexIds().size() > 1;
    } else {
      // the only element in the set of super vertex ids is the vertex id itself
      return (superVertexGroupItem.getVertexIds().size() == 1) &&
        (superVertexGroupItem.getVertexIds().iterator().next().equals(
          superVertexGroupItem.getSuperVertexId()));
    }
  }
}
