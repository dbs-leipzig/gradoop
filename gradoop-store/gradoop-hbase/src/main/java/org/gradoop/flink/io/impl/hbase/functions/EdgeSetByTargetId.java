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
package org.gradoop.flink.io.impl.hbase.functions;

import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Takes grouped edges as input and outputs a tuple containing target vertex id
 * and the edges.
 *
 * edge+ -> (targetId, edge+)
 *
 * @param <E> EPGM edge type
 */
public class EdgeSetByTargetId<E extends Edge> extends EdgeSet<E> {

  /**
   * Constructor
   */
  public EdgeSetByTargetId() {
    extractBySourceId = false;
  }
}
