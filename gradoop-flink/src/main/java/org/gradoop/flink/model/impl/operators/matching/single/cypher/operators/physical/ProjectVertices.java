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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.ProjectVertexFunction;

import java.util.List;

/**
 * Projects a Vertex by a set of properties.
 * Vertex -> Embedding(GraphElementEmbedding(Vertex))
 */
public class ProjectVertices implements PhysicalOperator {

  /**
   * Input vertices
   */
  private final DataSet<Vertex> input;
  /**
   * Names of the properties that will be kept in the projection
   */
  private final List<String> propertyKeys;

  /**
   * Creates a new vertex projection operator
   * @param input vertices that should be projected
   * @param propertyKeys List of propertyKeys that will be kept in the projection
   */
  public ProjectVertices(DataSet<Vertex> input, List<String> propertyKeys) {
    this.input = input;
    this.propertyKeys = propertyKeys;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.map(new ProjectVertexFunction(propertyKeys));
  }
}
