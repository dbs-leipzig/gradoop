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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingEntry;

/**
 * Reverses an EdgeEmbedding, as it switches source and target
 * This is used for traversing incoming edges
 */
public class ReverseEdgeEmbedding extends RichMapFunction<Embedding, Embedding> {

  @Override
  public Embedding map(Embedding value) throws Exception {
    EmbeddingEntry tmp = value.getEntry(0);
    value.setEntry(0, value.getEntry(2));
    value.setEntry(2, tmp);
    return value;
  }
}
