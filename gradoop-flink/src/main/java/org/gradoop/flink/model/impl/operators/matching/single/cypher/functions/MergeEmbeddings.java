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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical.JoinEmbeddings;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Given two input embeddings, the function merges them according to the given parameters and
 * constraints.
 *
 * The constraints for merging are defined at {@link JoinEmbeddings}.
 */
public class MergeEmbeddings implements FlatJoinFunction<Embedding, Embedding, Embedding> {
  /**
   * Join columns from the right side.
   */
  private final List<Integer> joinColumnsRight;
  /**
   * Vertex columns of the left embedding that need to have distinct id values.
   */
  private final List<Integer> distinctVertexColumnsLeft;
  /**
   * Vertex columns of the right embedding that need to have distinct id values.
   */
  private final List<Integer> distinctVertexColumnsRight;
  /**
   * Edge columns of the left embedding that need to have distinct id values.
   */
  private final List<Integer> distinctEdgeColumnsLeft;
  /**
   * Edge columns of the right embedding that need to have distinct id values.
   */
  private final List<Integer> distinctEdgeColumnsRight;
  /**
   * Join columns that are adopted from the right side to the left side.
   */
  private final Map<Integer, Integer> adoptColumns;

  /**
   * Creates a new UDF instance.
   *
   * @param joinColumnsRight join columns of the right side
   * @param distinctVertexColumnsLeft distinct vertex columns of the left embedding
   * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
   * @param distinctEdgeColumnsLeft distinct edge columns of the left embedding
   * @param distinctEdgeColumnsRight distinct edge columns of the right embedding
   * @param adoptColumns columns that are adopted from the right side to the left side
   */
  public MergeEmbeddings(List<Integer> joinColumnsRight,
    List<Integer> distinctVertexColumnsLeft,
    List<Integer> distinctVertexColumnsRight,
    List<Integer> distinctEdgeColumnsLeft,
    List<Integer> distinctEdgeColumnsRight,
    Map<Integer, Integer> adoptColumns) {
    this.joinColumnsRight           = joinColumnsRight;
    this.adoptColumns               = adoptColumns;
    this.distinctVertexColumnsLeft  = distinctVertexColumnsLeft;
    this.distinctVertexColumnsRight = distinctVertexColumnsRight;
    this.distinctEdgeColumnsLeft    = distinctEdgeColumnsLeft;
    this.distinctEdgeColumnsRight   = distinctEdgeColumnsRight;
  }

  @Override
  public void join(Embedding left, Embedding right, Collector<Embedding> out) throws Exception {
    // 1 check distinct columns
    if (isDistinct(distinctVertexColumnsLeft, distinctVertexColumnsRight, left, right) &&
      isDistinct(distinctEdgeColumnsLeft, distinctEdgeColumnsRight, left, right)) {

      // 2 adopt columns from the right side
      for (Map.Entry<Integer, Integer> adoptColumn : adoptColumns.entrySet()) {
        left.setEntry(adoptColumn.getValue(), right.getEntry(adoptColumn.getKey()));
      }

      // 3 append new elements from the right side
      List<EmbeddingEntry> rightEntries = right.getEntries();
      for (int i = 0; i < rightEntries.size(); i++) {
        if (!joinColumnsRight.contains(i)) {
          left.addEntry(right.getEntry(i));
        }
      }
      // 4 collect merged embedding
      out.collect(left);
    }
  }

  /**
   * Checks if both given embeddings contain distinct id values at all specified columns.
   *
   * @param columnsLeft left columns to include in check for uniqueness
   * @param columnsRight right columns to include in check for uniqueness
   * @param left left embedding
   * @param right right embedding
   * @return true, if both embeddings contain unique id values for all specified columns
   */
  private boolean isDistinct(List<Integer> columnsLeft, List<Integer> columnsRight,
    Embedding left, Embedding right) {

    Set<GradoopId> ids = new HashSet<>(left.size() + right.size());
    return isDistinct(ids, columnsLeft, left) && isDistinct(ids, columnsRight, right);
  }

  /**
   * Checks if the specified embeddings contains distinct ids at the specified columns.
   *
   * @param ids used to track uniqueness
   * @param columns columns to check for uniqueness
   * @param embedding embedding to check
   * @return true, if the embedding contains distinct Ids at the specified columns
   */
  private boolean isDistinct(Set<GradoopId> ids, List<Integer> columns, Embedding embedding) {
    boolean isDistinct = true;
    for (Integer column : columns) {
      isDistinct = ids.add(embedding.getEntry(column).getId());
      if (!isDistinct) {
        break;
      }
    }
    return isDistinct;
  }
}
