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

import org.gradoop.flink.model.impl.operators.grouping.functions.BuildBase;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;

/**
 * Base class for reducer/combiner implementations on edges.
 */
abstract class ReduceSuperEdgeGroupItemBase extends BuildBase {

  /**
   * Reduce object instantiation.
   */
  private SuperEdgeGroupItem reuseSuperEdgeGroupItem;
  /**
   * True if the source shall be considered for grouping.
   */
  private boolean sourceSpecificGrouping;
  /**
   * True if the target shall be considered for grouping.
   */
  private boolean targetSpecificGrouping;

  /**
   * Creates build base.
   *
   * @param useLabel true, if element label shall be used for grouping
   * @param sourceSpecificGrouping true if the source vertex shall be considered for grouping
   * @param targetSpecificGrouping true if the target vertex shall be considered for grouping
   */
  protected ReduceSuperEdgeGroupItemBase(boolean useLabel, boolean sourceSpecificGrouping,
    boolean targetSpecificGrouping) {
    super(useLabel);
    this.reuseSuperEdgeGroupItem = new SuperEdgeGroupItem();
    this.sourceSpecificGrouping = sourceSpecificGrouping;
    this.targetSpecificGrouping = targetSpecificGrouping;
  }

  protected SuperEdgeGroupItem getReuseSuperEdgeGroupItem() {
    return this.reuseSuperEdgeGroupItem;
  }

  public void setReuseSuperEdgeGroupItem(SuperEdgeGroupItem reuseSuperEdgeGroupItem) {
    this.reuseSuperEdgeGroupItem = reuseSuperEdgeGroupItem;
  }

  protected boolean isSourceSpecificGrouping() {
    return sourceSpecificGrouping;
  }

  protected boolean isTargetSpecificGrouping() {
    return targetSpecificGrouping;
  }

}
