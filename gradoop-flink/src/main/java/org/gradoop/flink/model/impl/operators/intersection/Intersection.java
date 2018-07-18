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
package org.gradoop.flink.model.impl.operators.intersection;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.base.SetOperatorBase;
import org.gradoop.flink.model.impl.operators.intersection.functions.GroupCountEquals;

/**
 * Returns a collection with all logical graphs that exist in both input
 * collections. Graph equality is based on their identifiers.
 *
 * @see IntersectionBroadcast
 */
public class Intersection extends SetOperatorBase {

  /**
   * Computes new subgraphs by grouping both graph collections by graph
   * identifier and returning those graphs where the group contains more
   * than one element.
   *
   * @return subgraph dataset of the resulting collection
   */
  @Override
  protected DataSet<GraphHead> computeNewGraphHeads() {
    return firstCollection.getGraphHeads()
      .union(secondCollection.getGraphHeads())
      .groupBy(new Id<GraphHead>())
      .reduceGroup(new GroupCountEquals<GraphHead>(2));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Intersection.class.getName();
  }
}
