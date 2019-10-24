/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.grouping;

import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;

/**
 * Used to define the grouping strategy which is used for computing the summary graph.
 */
public enum GroupingStrategy {
  /**
   * Grouping group reduce strategy.
   *
   * @see GroupingGroupReduce
   */
  GROUP_REDUCE,
  /**
   * Grouping group combine strategy.
   *
   * @see GroupingGroupCombine
   */
  GROUP_COMBINE,
  /**
   * The grouping implementation based on tuples and key functions.
   *
   * @see KeyedGrouping
   */
  GROUP_WITH_KEYFUNCTIONS
}
