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

package org.gradoop.flink.model.impl.operators.distinct;

import org.gradoop.flink.model.api.operators
  .UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Returns a distinct collection of logical graphs. Graph heads are compared
 * based on their identifier.
 */
public class Distinct implements UnaryCollectionToCollectionOperator {

  @Override
  public GraphCollection execute(GraphCollection collection) {
    return GraphCollection.fromDataSets(
      collection.getGraphHeads().distinct(new Id<GraphHead>()),
      collection.getVertices(),
      collection.getEdges(),
      collection.getConfig()
    );
  }

  @Override
  public String getName() {
    return Distinct.class.getName();
  }
}
