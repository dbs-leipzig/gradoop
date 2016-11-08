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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;

import java.util.HashMap;
import java.util.List;

/**
 * Projects an Embedding by a set of properties.
 * For each entry in the embedding a different property set can be specified
 */
public class ProjectEmbeddings implements PhysicalOperator {

  private final DataSet<Embedding> input;
  private final HashMap<Integer,List<String>> propertyKeys;

  /**
   * Creates a new embedding projection operator
   * @param input Embeddings that should be projected
   * @param propertyKeys HashMap of property labels, keys are the columns of the entry, values are property keys
   */
  public ProjectEmbeddings(DataSet<Embedding> input, HashMap<Integer,List<String>> propertyKeys) {
    this.input = input;
    this.propertyKeys = propertyKeys;
  }

  public DataSet<Embedding> evaluate() {
    return null;
  }
}
