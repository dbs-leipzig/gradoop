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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.physical_operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.s1ck.gdl.model.cnf.CNF;

import java.util.List;

/**
 * Filters a List of Vertices by predicates and projects the remaining vertices to the specified properties
 * Returns one {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Projection} entry
 */
public class FilterAndProjectVertices implements PhysicalOperator{

  private final DataSet<Vertex> input;
  private final CNF predicates;
  private final List<String> propertyKeys;

  /**
   * New Operator
   *
   * @param input Candidate vertices
   * @param predicates Predicates that will be used to filter candidate vertices
   * @param propertyKeys List of property keys that will be used for projection
   */
  public FilterAndProjectVertices(DataSet<Vertex> input, CNF predicates, List<String> propertyKeys) {
    this.input = input;
    this.predicates = predicates;
    this.propertyKeys = propertyKeys;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
