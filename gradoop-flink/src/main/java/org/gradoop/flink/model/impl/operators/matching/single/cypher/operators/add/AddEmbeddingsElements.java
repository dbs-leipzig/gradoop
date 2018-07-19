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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.add;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.add.functions.AddEmbeddingElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.List;

/**
 * Adds {@link Embedding} entries to a given {@link Embedding} based on a {@link List}
 * of variables via an {@link EmbeddingMetaData} object.
 */
public class AddEmbeddingsElements implements PhysicalOperator {
  /**
   * Input embeddings
   */
  private final DataSet<Embedding> input;
  /**
   * Number of elements to add to the embedding.
   */
  private final int count;
  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * New embeddings add operator
   *
   * @param input input embeddings
   * @param count number of elements to add
   */
  public AddEmbeddingsElements(DataSet<Embedding> input, int count) {
    this.input = input;
    this.count = count;
    this.setName("AddEmbeddingsElements");
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.map(new AddEmbeddingElements(count));
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
