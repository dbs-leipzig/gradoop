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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Filter to check if the current count for all visited vertices is smaller than the number of
 * vertices to visit. Empties the dataset, if not. This filter is applied to the dataset used for
 * the iteration termination criterion. The iteration terminates if the regarding dataset is empty.
 */
public class CheckIfVerticesToVisitLeftFilter implements FilterFunction<IterativeTuple> {

  /**
   * Number of vertices to visit
   */
  private final long toVisitCount;

  /**
   * Creates an instance of CheckIfVerticesToVisitLeftFilter
   *
   * @param toVisitCount Number of vertices to visit
   */
  public CheckIfVerticesToVisitLeftFilter(long toVisitCount) {
    this.toVisitCount = toVisitCount;
  }

  @Override
  public boolean filter(IterativeTuple iterativeTuple) throws Exception {
    return iterativeTuple.getVisitedCount() < toVisitCount;
  }
}
