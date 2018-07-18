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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectVertex;

import java.util.ArrayList;
import java.util.List;

/**
 * Projects a Vertex by a set of properties.
 * Vertex -> Embedding(ProjectionEmbedding(Vertex))
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
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * Creates a new vertex projection operator
   *
   * @param input vertices that should be projected
   * @param propertyKeys List of propertyKeys that will be kept in the projection
   */
  public ProjectVertices(DataSet<Vertex> input, List<String> propertyKeys) {
    this.input = input;
    this.propertyKeys = propertyKeys;
    this.name = "ProjectVertices";
  }

  /**
   * Creates a new vertex projection operator wih empty property list
   * Evaluate will return Embedding(IDEntry)
   *
   * @param input vertices that will be projected
   */
  public ProjectVertices(DataSet<Vertex> input) {
    this.input = input;
    this.propertyKeys = new ArrayList<>();
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input
      .map(new ProjectVertex(propertyKeys))
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
