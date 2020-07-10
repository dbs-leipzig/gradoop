/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project;

import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectTemporalVertex;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.ArrayList;
import java.util.List;

/**
 * Projects a TPGM Vertex by a set of properties.
 * <p>
 * {@code TemporalVertex -> EmbeddingTPGM(ProjectionEmbedding(Vertex))}
 */
public class ProjectTemporalVertices implements PhysicalTPGMOperator {

  /**
   * Input vertices
   */
  private final DataSet<TemporalVertex> input;
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
   * @param input        vertices that should be projected
   * @param propertyKeys List of propertyKeys that will be kept in the projection
   */
  public ProjectTemporalVertices(DataSet<TemporalVertex> input, List<String> propertyKeys) {
    this.input = input;
    this.propertyKeys = propertyKeys;
    this.name = "ProjectVertices";
  }

  /**
   * Creates a new vertex projection operator wih empty property list
   * Evaluate will return EmbeddingTPGM(IDEntry)
   *
   * @param input vertices that will be projected
   */
  public ProjectTemporalVertices(DataSet<TemporalVertex> input) {
    this.input = input;
    this.propertyKeys = new ArrayList<>();
  }

  @Override
  public DataSet<EmbeddingTPGM> evaluate() {
    return input
      .map(new ProjectTemporalVertex(propertyKeys))
      .name(getName());
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public void setName(String newName) {
    this.name = newName;
  }
}
