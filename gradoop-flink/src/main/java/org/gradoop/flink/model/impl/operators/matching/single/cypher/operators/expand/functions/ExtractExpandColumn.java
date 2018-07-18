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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Extracts a join key from an id stored in an embedding record
 * The id is referenced via its column index.
 */
public class ExtractExpandColumn implements KeySelector<Embedding, GradoopId> {
  /**
   * Column that holds the id which will be used as key
   */
  private final Integer column;

  /**
   * Creates the key selector
   *
   * @param column column that holds the id which will be used as key
   */
  public ExtractExpandColumn(Integer column) {
    this.column = column;
  }

  @Override
  public GradoopId getKey(Embedding value) throws Exception {
    return value.getId(column);
  }
}
