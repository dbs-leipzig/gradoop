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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.add;

import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.add.functions.AddEmbeddingTPGMElements;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

import java.util.List;

/**
 * Adds {@link EmbeddingTPGM} entries to a given {@link EmbeddingTPGM} based on a {@link List}
 * of variables via an {@link EmbeddingTPGMMetaData} object.
 */
public class AddEmbeddingsTPGMElements implements PhysicalTPGMOperator {
  /**
   * Input embeddings
   */
  private final DataSet<EmbeddingTPGM> input;
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
  public AddEmbeddingsTPGMElements(DataSet<EmbeddingTPGM> input, int count) {
    this.input = input;
    this.count = count;
    this.setName("AddEmbeddingsElements");
  }

  @Override
  public DataSet<EmbeddingTPGM> evaluate() {
    return input.map(new AddEmbeddingTPGMElements(count));
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
