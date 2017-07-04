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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Filters elements if their identifier is not equal to the given identifier.
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ReadFields("id")
public class ByDifferentId<EL extends Element>
  implements FilterFunction<EL> {

  /**
   * id
   */
  private final GradoopId id;

  /**
   * Creates new filter instance.
   *
   * @param id identifier
   */
  public ByDifferentId(GradoopId id) {
    this.id = id;
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return !element.getId().equals(id);
  }
}
