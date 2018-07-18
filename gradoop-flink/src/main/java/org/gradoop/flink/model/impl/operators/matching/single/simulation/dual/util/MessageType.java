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
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.util;

import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples
  .Deletion;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples
  .Message;

/**
 * Message types used in {@link Deletion} and {@link Message}.
 */
public enum MessageType {
  /**
   * Message is sent from the vertex to itself.
   */
  FROM_SELF,
  /**
   * Message is sent from a child vertex.
   */
  FROM_CHILD,
  /**
   * Message is sent from a parent vertex.
   */
  FROM_PARENT,
  /**
   * Message is sent from a child vertex which will be removed at the end of
   * the iteration.
   */
  FROM_CHILD_REMOVE,
  /**
   * Message is sent from a parent vertex which will be removed at the end of
   * the iteration.
   */
  FROM_PARENT_REMOVE
}
