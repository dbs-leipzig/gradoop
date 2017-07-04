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
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * True, if an element is contained in any of a set of given graphs.
 *
 * @param <GE> element type
 */
@FunctionAnnotation.ReadFields("graphIds")
public class InAnyGraph<GE extends GraphElement>
  implements FilterFunction<GE> {

  /**
   * graph ids
   */
  private final GradoopIdList graphIds;

  /**
   * constructor
   * @param graphIds graph ids
   */
  public InAnyGraph(GradoopIdList graphIds) {
    this.graphIds = graphIds;
  }

  @Override
  public boolean filter(GE element) throws Exception {
    return element.getGraphIds().containsAny(this.graphIds);
  }
}
