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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.Map;

/**
 * Projects {@link Embedding} entry columns based on a column-to-column mapping
 */
public class ProjectEmbeddingElements implements MapFunction<Embedding, Embedding> {
  /**
   * A mapping from input column to output column
   */
  private final Map<Integer, Integer> projectionColumns;
  /**
   * New embeddings filter function
   *
   * @param projectionColumns Variables to keep in output embedding
   */
  public ProjectEmbeddingElements(Map<Integer, Integer> projectionColumns) {
    this.projectionColumns = projectionColumns;
  }

  @Override
  public Embedding map(Embedding embedding) throws Exception {
    GradoopId[] idField = new GradoopId[projectionColumns.size()];

    for (Map.Entry<Integer, Integer> projection : projectionColumns.entrySet()) {
      idField[projection.getValue()] = embedding.getId(projection.getKey());
    }

    Embedding newEmbedding = new Embedding();
    newEmbedding.addAll(idField);
    return newEmbedding;
  }
}
