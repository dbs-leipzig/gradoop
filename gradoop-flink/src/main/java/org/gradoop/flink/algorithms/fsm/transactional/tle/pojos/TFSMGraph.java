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
package org.gradoop.flink.algorithms.fsm.transactional.tle.pojos;

import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Lightweight representation of a labeled graph transaction.
 */
public class TFSMGraph extends Embedding implements FSMGraph {

  /**
   * graph identifier
   */
  private final GradoopId id;

  /**
   * Constructor.
   *
   * @param id graph identifier
   * @param vertices id-vertex map
   * @param edges id-edge map
   */
  public TFSMGraph(GradoopId id,
    Map<Integer, String> vertices, Map<Integer, FSMEdge> edges) {
    super(vertices, edges);
    this.id = id;
  }

  public GradoopId getId() {
    return id;
  }
}
