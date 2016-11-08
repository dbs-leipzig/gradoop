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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;

import java.util.List;

/**
 * Projects a set of edges
 * The returned embedding consists of 3 entries Id(sourceID), Projection(edge), Id(targetId)
 */
public class ProjectEdges implements PhysicalOperator {

  private final DataSet<Edge> input;
  private final List<String> propertyKeys;

  /**
   * New edge projection operator
   * @param input Candidate edges
   * @param propertyKeys List of property keys that will be included in the projection
   */
  public ProjectEdges(DataSet<Edge> input, List<String> propertyKeys) {
    this.input = input;
    this.propertyKeys = propertyKeys;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
