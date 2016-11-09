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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.PropertyProjector;

import java.util.List;

/**
 * Converts a vertex into an Embedding containing one ProjectionEntry including all specified
 * properties.
 * vertex -> Embedding(ProjectionEntry)
 */
public class VertexProjector extends RichMapFunction<Vertex, Embedding> {
  /**
   * Specifies properties included in projection
   */
  private List<String> keys;
  /**
   * Stores a reuse projector
   */
  private PropertyProjector projector;

  /**
   * Create a new VertexProjector
   * @param keys properties included in projection
   */
  public VertexProjector(List<String> keys) {
    this.keys = keys;
  }

  @Override
  public void open(Configuration configuration) {
    this.projector = new PropertyProjector(keys);
  }

  public Embedding map(Vertex vertex) {
    Embedding embedding = new Embedding();
    embedding.addEntry(projector.project(vertex));
    return embedding;
  }
}