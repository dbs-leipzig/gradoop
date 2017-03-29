/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
