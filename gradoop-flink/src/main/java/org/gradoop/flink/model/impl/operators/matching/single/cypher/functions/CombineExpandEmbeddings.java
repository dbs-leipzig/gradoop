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

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdListEntry;

import java.util.List;

/**
 * Expands a given Embedding by an Edge
 */
public class CombineExpandEmbeddings
  extends RichFlatJoinFunction<Embedding,Embedding,Embedding> {

  /**
   * Holds the index of all vertex columns that should be distinct
   */
  protected final List<Integer> distinctVertices;
  /**
   * Holds the index of all edge columns that should be distinct
   */
  protected final List<Integer> distinctEdges;

  /**
   * Create a new Combine Expand Embeddings Operator
   * @param distinctVertices distinct vertex columns
   * @param distinctEdges distinct edge columns
   */
  public CombineExpandEmbeddings(List<Integer> distinctVertices, List<Integer> distinctEdges) {
    this.distinctVertices = distinctVertices;
    this.distinctEdges = distinctEdges;
  }

  @Override
  public void join(Embedding base, Embedding extension, Collector<Embedding> out) throws Exception {
    if (checkExistence(extension.getEntry(2).getId(), distinctVertices, base) ||
        checkExistence(extension.getEntry(1).getId(), distinctEdges, base)) {
      return;
    }

    Embedding newEmbedding = new Embedding();
    newEmbedding.addEntries(base.getEntries());

    IdListEntry newIdList = new IdListEntry();
    newIdList.addIds(((IdListEntry) base.getEntry(1)).getIds());
    newIdList.addId(extension.getEntry(0).getId());
    newIdList.addId(extension.getEntry(1).getId());

    newEmbedding.addEntry(newIdList);

    newEmbedding.addEntry(extension.getEntry(2));

    out.collect(newEmbedding);
  }

  /**
   * Checks if the id is present in one of the distinct columns
   * @param id the id to be checked
   * @param distinctColumns the columns which should be distinct
   * @param base the embedding the columns refer to
   * @return true if the id exists in on if the distinct columns
   */
  boolean checkExistence(GradoopId id, List<Integer> distinctColumns, Embedding base) {
    for (int vertexColumn : distinctColumns) {
      if (vertexColumn < base.size()) {
        if (base.getEntry(vertexColumn).contains(id)) return false;
      }
    }
    return false;
  }
}