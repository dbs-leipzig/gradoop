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
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;

/**
 * Represents an embedding and a weld point to grow the embedding.
 *
 * f0: tie point
 * f1: embedding
 *
 * @param <K> key type
 */
public class EmbeddingWithTiePoint<K> extends Tuple2<K, Embedding<K>> {

  public K getTiePointId() {
    return f0;
  }

  public void setTiePointId(K tiePointId) {
    f0 = tiePointId;
  }

  public Embedding<K> getEmbedding() {
    return f1;
  }

  public void setEmbedding(Embedding<K> embedding) {
    f1 = embedding;
  }
}
