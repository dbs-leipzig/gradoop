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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectEmbedding;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Projects an Embedding by a set of properties.
 * For each entry in the embedding a different property set can be specified
 */
public class ProjectEmbeddings implements PhysicalOperator {
  /**
   * Input Embeddings
   */
  private final DataSet<Embedding> input;
  /**
   * Indices of all properties that will be kept in the projection
   */
  private final List<Integer> propertyWhiteList;

  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * Creates a new embedding projection operator
   * @param input Embeddings that should be projected
   * @param propertyWhiteList property columns in the embedding that are taken over to the output
   */
  public ProjectEmbeddings(DataSet<Embedding> input, List<Integer> propertyWhiteList) {
    this.input = input;
    this.propertyWhiteList = propertyWhiteList.stream().sorted().collect(Collectors.toList());
    this.name = "ProjectEmbeddings";
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input
      .map(new ProjectEmbedding(propertyWhiteList))
      .name(getName());
  }

  @Override
  public void setName(String newName) {
    this.name = newName;
  }

  @Override
  public String getName() {
    return this.name;
  }
}
