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
/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or transform
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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.exclusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.functions.utils.LeftWhenRightIsNull;

/**
 * Computes the exclusion graph from two logical graphs.
 * Reduces the first input graph to contain only vertices and edges that don't exist in
 * the second graph. The graph head of the first graph is retained. Vertex and edge equality
 * is based on their respective identifiers.
 */
public class Exclusion implements BinaryGraphToGraphOperator {

  @Override
  public LogicalGraph execute(
    LogicalGraph firstGraph, LogicalGraph secondGraph) {
    DataSet<Vertex> newVertexSet = firstGraph.getVertices()
      .leftOuterJoin(secondGraph.getVertices())
      .where(new Id<>())
      .equalTo(new Id<>())
      .with(new LeftWhenRightIsNull<>());

    DataSet<Edge> newEdgeSet = firstGraph.getEdges()
      .join(newVertexSet)
      .where(new SourceId<>())
      .equalTo(new Id<>())
      .with(new LeftSide<>())
      .join(newVertexSet)
      .where(new TargetId<>())
      .equalTo(new Id<>())
      .with(new LeftSide<>());

    return firstGraph.getFactory()
      .fromDataSets(firstGraph.getGraphHead(), newVertexSet, newEdgeSet);
  }
}
