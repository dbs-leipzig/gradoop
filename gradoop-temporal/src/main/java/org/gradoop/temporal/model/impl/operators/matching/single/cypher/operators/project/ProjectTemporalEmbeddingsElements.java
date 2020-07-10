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
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectTemporalEmbeddingElements;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Projects elements from the input {@link EmbeddingTPGM} into an output {@link EmbeddingTPGM} based on
 * a given set of variables.
 */
public class ProjectTemporalEmbeddingsElements implements PhysicalTPGMOperator {
  /**
   * Input graph elements
   */
  private final DataSet<EmbeddingTPGM> input;
  /**
   * Return pattern variables that already exist in pattern matching query
   */
  private final Set<String> projectionVariables;
  /**
   * Meta data for input embeddings
   */
  private final EmbeddingTPGMMetaData inputMetaData;
  /**
   * Meta data for output embeddings
   */
  private final EmbeddingTPGMMetaData outputMetaData;
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
  public ProjectTemporalEmbeddingsElements(DataSet<EmbeddingTPGM> input,
                                           Set<String> projectionVariables,
                                           EmbeddingTPGMMetaData inputMetaData,
                                           EmbeddingTPGMMetaData outputMetaData) {
    this.input = input;
    this.projectionVariables = projectionVariables;
    this.inputMetaData = inputMetaData;
    this.outputMetaData = outputMetaData;
    this.setName("ProjectEmbeddingsElements");
  }

  @Override
  public DataSet<EmbeddingTPGM> evaluate() {
    Map<Integer, Integer> projection = projectionVariables.stream()
      .collect(Collectors.toMap(inputMetaData::getEntryColumn, outputMetaData::getEntryColumn));

    return input.map(new ProjectTemporalEmbeddingElements(projection));
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
