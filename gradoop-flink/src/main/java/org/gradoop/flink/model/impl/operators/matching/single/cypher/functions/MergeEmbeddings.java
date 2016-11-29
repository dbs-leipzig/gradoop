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
   * Vertex columns that need to have distinct id values.
   */
  private final List<Integer> distinctVertexColumns;
  /**
   * Edge columns that need to have distinct id values.
   */
  private final List<Integer> distinctEdgeColumns;
  /**
   * Join columns that are adopted from the right side to the left side.
   */
  private final Map<Integer, Integer> adoptColumns;

  public MergeEmbeddings(List<Integer> joinColumnsRight,
    List<Integer> distinctVertexColumns,
    List<Integer> distinctEdgeColumns,
    Map<Integer, Integer> adoptColumns) {
    this.joinColumnsRight = joinColumnsRight;
    this.adoptColumns = adoptColumns;
    this.distinctVertexColumns = distinctVertexColumns;
    this.distinctEdgeColumns = distinctEdgeColumns;
  }

  @Override
  public void join(Embedding left, Embedding right, Collector<Embedding> out) throws Exception {
    // 1 adopt columns from the right side
    for (Map.Entry<Integer, Integer> adoptColumn : adoptColumns.entrySet()) {
      left.setEntry(adoptColumn.getValue(), right.getEntry(adoptColumn.getKey()));
    }

    // 2 append new elements from the right side
    List<EmbeddingEntry> rightEntries = right.getEntries();
    for (int i = 0; i < rightEntries.size(); i++) {
      if (!joinColumnsRight.contains(i)) {
        left.addEntry(right.getEntry(i));
      }
    }

    // 3 check distinct columns
    if (isDistinct(distinctVertexColumns, left) && isDistinct(distinctEdgeColumns, left)) {
      out.collect(left);
    }
  }

  /**
   * Checks if the specified embedding contains distinct Ids at the specified columns.
   *
   * @param columns columns to check for uniqueness
   * @param embedding embedding to check
   * @return true, if the Ids at the specified columns are distinct
   */
  private boolean isDistinct(List<Integer> columns, Embedding embedding) {
    Set<GradoopId> ids = new HashSet<>(embedding.size());
    boolean distinct = true;
    for (Integer column : columns) {
      distinct = ids.add(embedding.getEntry(column).getId());
      if (!distinct) {
        break;
      }
    }
    return distinct;
  }
}
