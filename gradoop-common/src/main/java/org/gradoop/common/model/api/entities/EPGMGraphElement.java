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
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * A graph element is part of a logical graph. An element can be part of more
 * than one logical graph. This applies to vertices and edges in the EPGM.
 */
public interface EPGMGraphElement extends EPGMElement {
  /**
   * Returns all graphs that element belongs to.
   *
   * @return all graphs of that element
   */
  GradoopIdSet getGraphIds();

  /**
   * Adds that element to the given graphId. If the element is already an
   * element of the given graphId, nothing happens.
   *
   * @param graphId the graphId to be added to
   */
  void addGraphId(GradoopId graphId);

  /**
   * Adds the given graph set to the element.
   *
   * @param graphIds the graphIds to be added
   */
  void setGraphIds(GradoopIdSet graphIds);

  /**
   * Resets all graph elements.
   */
  void resetGraphIds();

  /**
   * Returns the number of graphs this element belongs to.
   *
   * @return number of graphs containing that element
   */
  int getGraphCount();
}
