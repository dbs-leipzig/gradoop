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

import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdListEntry;

import java.util.List;

/**
 * Creates the initial expand embeddings
 * input x edges => ([input], IDList(edge.id), IdEntry(edge.src))
 */
public class CreateInitialExpandEmbedding extends CombineExpandEmbeddings {

  /**
   * Create new FlatJoin Function
   * @param distinctVertices indices of distinct vertex columns
   * @param distinctEdges indices of distinct edge columns
   */
  public CreateInitialExpandEmbedding(List<Integer> distinctVertices, List<Integer> distinctEdges) {
    super(distinctVertices, distinctEdges);
  }

  @Override
  public void join(Embedding base, Embedding edge, Collector<Embedding> out) throws Exception {
    if (checkExistence(edge.getEntry(2).getId(), distinctVertices, base) ||
        checkExistence(edge.getEntry(1).getId(), distinctEdges, base)) {
      return;
    }

    Embedding newEmbedding = new Embedding();

    // add the entries from base embedding
    newEmbedding.addEntries(base.getEntries());
    // create an IdListEntry with the edges id
    newEmbedding.addEntry(new IdListEntry(Lists.newArrayList(edge.getEntry(1).getId())));
    // create add edges target id
    newEmbedding.addEntry(edge.getEntry(2));

    out.collect(newEmbedding);
  }
}
