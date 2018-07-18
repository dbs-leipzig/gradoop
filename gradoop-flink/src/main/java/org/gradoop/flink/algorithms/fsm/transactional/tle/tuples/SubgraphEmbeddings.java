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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;

import java.util.List;

/**
 * Describes a subgraph and its embeddings in a certain graph.
 */
public interface SubgraphEmbeddings {

  /**
   * Getter.
   *
   * @return graph id
   */
  GradoopId getGraphId();

  /**
   * Setter.
   *
   * @param graphId graph id
   */
  void setGraphId(GradoopId graphId);

  /**
   * Getter.
   *
   * @return edge count
   */
  Integer getSize();

  /**
   * Setter.
   *
   * @param size edge count
   */
  void setSize(Integer size);

  /**
   * Getter.
   *
   * @return canonical label
   */
  String getCanonicalLabel();

  /**
   * Setter.
   *
   * @param label canonical label
   */
  void setCanonicalLabel(String label);

  /**
   * Getter.
   *
   * @return embeddings
   */
  List<Embedding> getEmbeddings();

  /**
   * Setter.
   *
   * @param embeddings embeddings
   */
  void setEmbeddings(List<Embedding> embeddings);
}
