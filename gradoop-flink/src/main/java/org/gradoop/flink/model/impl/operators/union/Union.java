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
package org.gradoop.flink.model.impl.operators.union;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.base.SetOperatorBase;

/**
 * Returns a collection with all logical graphs from two input collections.
 * Graph equality is based on their identifiers.
 */
public class Union extends SetOperatorBase {

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<Vertex> computeNewVertices(
    DataSet<GraphHead> newGraphHeads) {
    return firstCollection.getVertices()
      .union(secondCollection.getVertices())
      .distinct(new Id<Vertex>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<GraphHead> computeNewGraphHeads() {
    return firstCollection.getGraphHeads()
      .union(secondCollection.getGraphHeads())
      .distinct(new Id<GraphHead>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<Edge> computeNewEdges(DataSet<Vertex> newVertices) {
    return firstCollection.getEdges()
      .union(secondCollection.getEdges())
      .distinct(new Id<Edge>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Union.class.getName();
  }
}
