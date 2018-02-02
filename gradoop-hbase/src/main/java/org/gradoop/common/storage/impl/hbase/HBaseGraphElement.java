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
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Wraps an EPGM graph element data entity.
 *
 * @param <T> entity type
 */
public abstract class HBaseGraphElement<T extends EPGMGraphElement>
  extends HBaseElement<T> implements EPGMGraphElement {

  /**
   * Creates an EPGM graph element.
   *
   * @param epgmGraphElement encapsulated graph element
   */
  protected HBaseGraphElement(T epgmGraphElement) {
    super(epgmGraphElement);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIdSet getGraphIds() {
    return getEpgmElement().getGraphIds();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addGraphId(GradoopId graphId) {
    getEpgmElement().addGraphId(graphId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setGraphIds(GradoopIdSet graphIds) {
    getEpgmElement().setGraphIds(graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resetGraphIds() {
    getEpgmElement().resetGraphIds();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getGraphCount() {
    return getEpgmElement().getGraphCount();
  }
}
