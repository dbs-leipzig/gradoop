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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.s1ck.gdl.model.cnf.CNF;

/**
 * Filters a set of Vertices by the given predicates
 * Returns an Embedding with one
 * {@link IdEntry} entry
 */
public class FilterVertices implements PhysicalOperator {
  /**
   * Input vertices
   */
  private final DataSet<Vertex> input;
  /**
   * Predicates in conjunctive normal form
   */
  private final CNF predicates;


  public FilterVertices(DataSet<Vertex> input, CNF predicates) {
    this.input = input;
    this.predicates = predicates;
  }

  public DataSet<Embedding> evaluate() {

    return null;
  }
}
