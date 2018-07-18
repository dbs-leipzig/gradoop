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
package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;

/**
 * Representation of a subgraph.
 *
 * (canonicalLabel, frequency, sample embedding)
 */
public class TFSMSubgraph
  extends Tuple3<String, Long, Embedding> implements Subgraph {

  /**
   * Default constructor.
   */
  public TFSMSubgraph() {
    super();
  }

  /**
   * Constructor.
   *
   * @param subgraph canonical label
   * @param frequency frequency
   * @param embedding sample embedding
   */
  public TFSMSubgraph(String subgraph, Long frequency, Embedding embedding) {
    super(subgraph, frequency, embedding);
  }

  public String getCanonicalLabel() {
    return f0;
  }

  public void setCanonicalLabel(String subgraph) {
    f0 = subgraph;
  }

  @Override
  public long getCount() {
    return f1;
  }

  @Override
  public void setCount(long frequency) {
    f1 = frequency;
  }

  public Embedding getEmbedding() {
    return f2;
  }

  public void setEmbedding(Embedding embedding) {
    f2 = embedding;
  }
}
