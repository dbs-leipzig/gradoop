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

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;

import java.util.List;

/**
 * Representation of a subgraph supported by a graph and all its local
 * embeddings.
 *
 * (category, graphId, size, canonicalLabel, embeddings)
 */
public class CCSSubgraphEmbeddings
  extends Tuple5<GradoopId, Integer, String, List<Embedding>, String>
  implements SubgraphEmbeddings {

  /**
   * Default constructor
   */
  public CCSSubgraphEmbeddings() {
  }


  public String getCategory() {
    return f4;
  }

  public void setCategory(String category) {
    f4 = category;
  }

  public GradoopId getGraphId() {
    return f0;
  }

  public void setGraphId(GradoopId graphId) {
    f0 = graphId;
  }

  public Integer getSize() {
    return f1;
  }

  public void setSize(Integer size) {
    f1 = size;
  }


  public String getCanonicalLabel() {
    return f2;
  }

  public void setCanonicalLabel(String label) {
    f2 = label;
  }

  public List<Embedding> getEmbeddings() {
    return f3;
  }

  public void setEmbeddings(List<Embedding> embeddings) {
    f3 = embeddings;
  }
}
