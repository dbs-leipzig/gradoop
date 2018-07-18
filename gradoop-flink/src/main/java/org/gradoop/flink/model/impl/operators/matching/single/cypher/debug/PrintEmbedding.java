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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.debug;

import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Debug output for {@link Embedding}.
 */
public class PrintEmbedding extends Printer<Embedding, GradoopId> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintEmbedding.class);
  /**
   * Meta data describing the current embedding.
   */
  private final EmbeddingMetaData embeddingMetaData;

  /**
   * Constructor.
   *
   * @param embeddingMetaData meta data for the embedding to print
   */
  public PrintEmbedding(EmbeddingMetaData embeddingMetaData) {
    this.embeddingMetaData = embeddingMetaData;
  }

  @Override
  protected String getDebugString(Embedding embedding) {
    String vertexMapping = embeddingMetaData.getVertexVariables().stream()
      .map(var -> String.format("%s : %s", var,
        vertexMap.get(embedding.getId(embeddingMetaData.getEntryColumn(var)))))
      .collect(Collectors.joining(", "));

    String edgeMapping = embeddingMetaData.getEdgeVariables().stream()
      .map(var -> String.format("%s : %s", var,
        edgeMap.get(embedding.getId(embeddingMetaData.getEntryColumn(var)))))
      .collect(Collectors.joining(", "));

    String pathMapping = embeddingMetaData.getPathVariables().stream()
      .map(var -> {
          List<GradoopId> path = embedding.getIdList(embeddingMetaData.getEntryColumn(var));
          List<PropertyValue> ids = new ArrayList<>();
          for (int i = 0; i < path.size(); i++) {
            if (i % 2 == 0) { // edge
              ids.add(edgeMap.get(path.get(i)));
            } else {
              ids.add(vertexMap.get(path.get(i)));
            }
          }
          return String.format("%s : %s", var, ids);
        })
      .collect(Collectors.joining(", "));

    return String.format("vertex-mapping: {%s}, edge-mapping: {%s}, path-mapping: {%s}",
      vertexMapping, edgeMapping, pathMapping);
  }


  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
