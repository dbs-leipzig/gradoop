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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectEdge;

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
   * Indicates if the edges is loop
   */
  private final boolean isLoop;

  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * Creates a new edge projection operator
   * @param input edges that should be projected
   * @param propertyKeys List of property names that will be kept in the projection
   * @param isLoop indicates if the edges is a loop
   */
  public ProjectEdges(DataSet<Edge> input, List<String> propertyKeys, boolean isLoop) {
    this.input = input;
    this.propertyKeys = propertyKeys;
    this.name = "ProjectEdges";
    this.isLoop = isLoop;
  }

  /**
   * Creates a new edge projection operator wih empty property list
   * Evaluate will return Embedding(IDEntry)
   *
   * @param input vertices that will be projected
   * @param isLoop indicates if the edges is a loop
   */
  public ProjectEdges(DataSet<Edge> input, boolean isLoop) {
    this(input, new ArrayList<>(), isLoop);
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input
      .map(new ProjectEdge(propertyKeys, isLoop))
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
