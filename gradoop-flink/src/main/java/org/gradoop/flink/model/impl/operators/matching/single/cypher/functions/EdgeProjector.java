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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.PropertyProjector;

import java.util.List;

/**
 * Converts an edge into an Embedding containing trhee ProjectionEntry including all specified
 * properties.
 * vertex -> Embedding(ProjectionEntry)
 */
public class EdgeProjector extends RichMapFunction<Edge, Embedding> {
  /**
   * Specifies properties included in projection
   */
  private List<String> keys;
  /**
   * Stores a reuse projector
   */
  private PropertyProjector projector;

  /**
   * Creates a new edge projector
   * @param keys properties that will be included in the projection
   */
  public EdgeProjector(List<String> keys) {
    this.keys=keys;
  }

  @Override
  public void open(Configuration configuration) {
    this.projector = new PropertyProjector(keys);
  }

  @Override
  public Embedding map(Edge edge) {
    Embedding embedding = new Embedding();
    embedding.addEntry(new IdEntry(edge.getSourceId()));
    embedding.addEntry(projector.project(edge));
    embedding.addEntry(new IdEntry(edge.getTargetId()));

    return embedding;
  }
}