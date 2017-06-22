/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildBase;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation
  .PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Base class for reducer/combiner implementations on edges.
 */
abstract class ReduceSuperEdgeGroupItemBase extends BuildBase {
  /**
   * Reduce instantiations
   */
  private final SuperEdgeGroupItem reuseSuperEdgeGroupItem;

  private boolean sourceSpecificGrouping;

  private boolean targetSpecificGrouping;

  /**
   * Creates build base.
   *
   * @param useLabel          true, if element label shall be used for grouping
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


  protected boolean isSourceSpecificGrouping() {
    return sourceSpecificGrouping;
  }

  protected boolean isTargetSpecificGrouping() {
    return targetSpecificGrouping;
  }

}
