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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectEmbeddingElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Projects elements from the input {@link Embedding} into an output {@link Embedding} based on
 * a given set of variables.
 */
public class ProjectEmbeddingsElements implements PhysicalOperator {
  /**
   * Input graph elements
   */
  private final DataSet<Embedding> input;
  /**
   * Return pattern variables that already exist in pattern matching query
   */
  private final Set<String> projectionVariables;
  /**
   * Meta data for input embeddings
   */
  private final EmbeddingMetaData inputMetaData;
  /**
   * Meta data for output embeddings
   */
  private final EmbeddingMetaData outputMetaData;
  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * New embeddings filter operator
   *
   * @param input               input embeddings
   * @param projectionVariables projection variables
   * @param inputMetaData       input meta data
   * @param outputMetaData      output meta data
   */
  public ProjectEmbeddingsElements(DataSet<Embedding> input,
    Set<String> projectionVariables,
    EmbeddingMetaData inputMetaData,
    EmbeddingMetaData outputMetaData) {
    this.input = input;
    this.projectionVariables = projectionVariables;
    this.inputMetaData = inputMetaData;
    this.outputMetaData = outputMetaData;
    this.setName("ProjectEmbeddingsElements");
  }

  @Override
  public DataSet<Embedding> evaluate() {
    Map<Integer, Integer> projection = projectionVariables.stream()
      .collect(Collectors.toMap(inputMetaData::getEntryColumn, outputMetaData::getEntryColumn));

    return input.map(new ProjectEmbeddingElements(projection));
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
