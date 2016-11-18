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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.ProjectEdgeFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Projects an Edge by a set of properties.
 * Edge -> Embedding(IdEntry(SrcID), GraphElementEntry(Edge), IdEntry(TargetID))
 */
public class ProjectEdges implements PhysicalOperator {

  /**
   * Input edge
   */
  private final DataSet<Edge> input;
  /**
   * Names of the properties that will be kept in the projection
   */
  private final List<String> propertyKeys;

  /**
   * Creates a new edge projection operator
   * @param input edges that should be projected
   * @param propertyKeys List of property names that will be kept in the projection
   */
  public ProjectEdges(DataSet<Edge> input, List<String> propertyKeys) {
    this.input = input;
    this.propertyKeys = propertyKeys;
  }

  /**
   * Creates a new edge projection operator wih empty property list
   * Evaluate will return Embedding(IDEntry)
   *
   * @param input vertices that will be projected
   */
  public ProjectEdges(DataSet<Edge> input) {
    this.input = input;
    this.propertyKeys = new ArrayList<>();
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.map(new ProjectEdgeFunction(propertyKeys));
  }
}
