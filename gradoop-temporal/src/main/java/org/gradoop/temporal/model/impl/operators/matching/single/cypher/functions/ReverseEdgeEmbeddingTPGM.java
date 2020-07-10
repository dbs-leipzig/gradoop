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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

/**
 * Reverses an edge embedding, as it switches source and target ids and timedata
 * This is used for traversing incoming edges
 */
public class ReverseEdgeEmbeddingTPGM extends RichMapFunction<EmbeddingTPGM, EmbeddingTPGM> {
  @Override
  public EmbeddingTPGM map(EmbeddingTPGM value) throws Exception {
    return value.reverse();
  }
}
