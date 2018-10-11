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
package org.gradoop.common.model.impl.pojo.temporal;

import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * TODO: descriptions
 */
public abstract class TemporalGraphElement extends TemporalElement implements EPGMGraphElement {

  /**
   * Set of graph identifiers that element is contained in
   */
  private GradoopIdSet graphIds;

  /**
   * Default constructor.
   */
  protected TemporalGraphElement() {
  }

  protected TemporalGraphElement(GradoopId id, String label, Properties properties,
    GradoopIdSet graphIds) {
    super(id, label, properties);
    this.graphIds = graphIds;
  }

  protected TemporalGraphElement(GradoopId id, String label, Properties properties,
    GradoopIdSet graphIds, long validFrom) {
    super(id, label, properties, validFrom);
    this.graphIds = graphIds;
  }

  protected TemporalGraphElement(GradoopId id, String label, Properties properties,
    GradoopIdSet graphIds, long validFrom, long validTo) {
    super(id, label, properties, validFrom, validTo);
    this.graphIds = graphIds;
  }

  @Override
  public GradoopIdSet getGraphIds() {
    return graphIds;
  }

  @Override
  public void addGraphId(GradoopId graphId) {
    if (graphIds == null) {
      graphIds = new GradoopIdSet();
    }
    graphIds.add(graphId);
  }

  @Override
  public void setGraphIds(GradoopIdSet graphIds) {
    this.graphIds = graphIds;
  }

  @Override
  public void resetGraphIds() {
    if (graphIds != null) {
      graphIds.clear();
    }
  }

  @Override
  public int getGraphCount() {
    return (graphIds != null) ? graphIds.size() : 0;
  }
}
