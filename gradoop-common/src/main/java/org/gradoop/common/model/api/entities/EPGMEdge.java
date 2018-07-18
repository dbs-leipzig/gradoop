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
package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Describes data assigned to an edge in the EPGM.
 */
public interface EPGMEdge extends EPGMGraphElement {
  /**
   * Returns the source vertex identifier.
   *
   * @return source vertex id
   */
  GradoopId getSourceId();

  /**
   * Sets the source vertex identifier.
   *
   * @param sourceId source vertex id
   */
  void setSourceId(GradoopId sourceId);

  /**
   * Returns the target vertex identifier.
   *
   * @return target vertex id
   */
  GradoopId getTargetId();

  /**
   * Sets the target vertex identifier.
   *
   * @param targetId target vertex id.
   */
  void setTargetId(GradoopId targetId);
}
