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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.JoinEmbeddings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

/**
 * Given two input embeddings, the function merges them according to the given parameters and
 * constraints.
 *
 * The constraints for merging are defined at {@link JoinEmbeddings}.
 */
public class MergeEmbeddings implements
  FlatJoinFunction<Embedding, Embedding, Embedding>,
  FlatMapFunction<Tuple2<Embedding, Embedding>, Embedding> {

  /**
   * Reduce object instantiations.
   */
  protected final Embedding reuseEmbedding;
  /**
   * Non-Join columns from the right side.
   */
  private final int[] nonJoinColumnsRight;
  /**
   * Number of join columns of the right side.
   */
  private final int joinColumnsRightSize;
  /**
   * Vertex columns of the left embedding that need to have distinct id values.
   */
  private final int[] distinctVertexColumnsLeft;
  /**
   * Vertex columns of the right embedding that need to have distinct id values.
   */
  private final int[] distinctVertexColumnsRight;
  /**
   * Edge columns of the left embedding that need to have distinct id values.
   */
  private final int[] distinctEdgeColumnsLeft;
  /**
   * Edge columns of the right embedding that need to have distinct id values.
   */
  private final int[] distinctEdgeColumnsRight;
  /**
   * Flag, if vertex distinctiveness needs to be checked.
   */
  private final boolean checkDistinctVertices;
  /**
   * Flag, if vertex distinctiveness needs to be checked.
   */
  private final boolean checkDistinctEdges;

  /**
   * Creates a new UDF instance.
   *
   * @param rightColumns number of columns in the right embedding
   * @param joinColumnsRight join columns of the right side
   * @param distinctVertexColumnsLeft distinct vertex columns of the left embedding
   * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
   * @param distinctEdgeColumnsLeft distinct edge columns of the left embedding
   * @param distinctEdgeColumnsRight distinct edge columns of the right embedding
   */
  public MergeEmbeddings(int rightColumns,
    List<Integer> joinColumnsRight,
    List<Integer> distinctVertexColumnsLeft,
    List<Integer> distinctVertexColumnsRight,
    List<Integer> distinctEdgeColumnsLeft,
    List<Integer> distinctEdgeColumnsRight) {

    this.nonJoinColumnsRight = IntStream.range(0, rightColumns)
      .filter(col -> !joinColumnsRight.contains(col))
      .toArray();
    this.joinColumnsRightSize = joinColumnsRight.size();

    ToIntFunction<Integer> f = i -> i;
    this.distinctVertexColumnsLeft = distinctVertexColumnsLeft.stream().mapToInt(f).toArray();
    this.distinctVertexColumnsRight = distinctVertexColumnsRight.stream().mapToInt(f).toArray();
    this.distinctEdgeColumnsLeft = distinctEdgeColumnsLeft.stream().mapToInt(f).toArray();
    this.distinctEdgeColumnsRight = distinctEdgeColumnsRight.stream().mapToInt(f).toArray();

    this.checkDistinctVertices = distinctVertexColumnsLeft.size() > 0 ||
      distinctVertexColumnsRight.size() > 0;
    this.checkDistinctEdges = distinctEdgeColumnsLeft.size() > 0 ||
      distinctEdgeColumnsRight.size() > 0;
    this.reuseEmbedding = new Embedding();
  }

  @Override
  public void join(Embedding left, Embedding right, Collector<Embedding> out) throws Exception {
    if (isValid(left, right)) {
      buildEmbedding(left, right);
      out.collect(reuseEmbedding);
    }
  }

  @Override
  public void flatMap(Tuple2<Embedding, Embedding> value, Collector<Embedding> out)
      throws Exception {
    join(value.f0, value.f1, out);
  }

  /**
   * Checks if the merged embedding would hold under morphism setting
   * @param left left embedding
   * @param right right embedding
   * @return true if the morphism condition holds
   */
  protected boolean isValid(Embedding left, Embedding right) {
    boolean collect = false;

    // Vertex-Homomorphism + Edge-Homomorphism
    if (!checkDistinctVertices && !checkDistinctEdges) {
      collect = true;
      // Vertex-Homomorphism + Edge-Isomorphism
    } else if (!checkDistinctVertices) {
      if (isDistinct(distinctEdgeColumnsLeft, distinctEdgeColumnsRight, left, right)) {
        collect = true;
      }
      // Vertex-Isomorphism + Edge-Isomorphism
    } else {
      if (isDistinct(distinctVertexColumnsLeft, distinctVertexColumnsRight, left, right) &&
        isDistinct(distinctEdgeColumnsLeft, distinctEdgeColumnsRight, left, right)) {
        collect = true;
      }
    }

    return collect;
  }

  /**
   * Merges left and right embeddings into {@link MergeEmbeddings#reuseEmbedding}
   * @param left left embedding
   * @param right right embedding
   */
  protected void buildEmbedding(Embedding left, Embedding right) {
    reuseEmbedding.setIdData(mergeIdData(left, right));
    reuseEmbedding.setPropertyData(mergePropertyData(left, right));
    reuseEmbedding.setIdListData(mergeIdListData(left, right));
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
  private boolean isDistinct(int[] columnsLeft, int[] columnsRight,
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
  private boolean isDistinct(Set<GradoopId> ids, int[] columns, Embedding embedding) {
    boolean isDistinct = true;
    for (int column : columns) {
      isDistinct = ids.addAll(embedding.getIdAsList(column));
      if (!isDistinct) {
        break;
      }
    }
    return isDistinct;
  }

  /**
   * Merges the idData columns of left and right
   * All entries of left are kept as well as all right entries which aren't join columns
   *
   * @param left the left hand side embedding
   * @param right the right hand side embedding
   * @return the merged data represented as byte array
   */
  private byte[] mergeIdData(Embedding left, Embedding right) {
    byte[] newIdData = new byte[
      left.getIdData().length +
      right.getIdData().length -
      (joinColumnsRightSize * (Embedding.ID_ENTRY_SIZE))
    ];

    int offset = left.getIdData().length;
    System.arraycopy(left.getIdData(), 0, newIdData, 0, offset);

    for (int i : nonJoinColumnsRight) {
      System.arraycopy(right.getRawIdEntry(i), 0, newIdData, offset, Embedding.ID_ENTRY_SIZE);
      offset += Embedding.ID_ENTRY_SIZE;
    }

    return newIdData;
  }

  /**
   * Merges the propertyData columns of the left and right embeddings
   * All entries from both sides are kept.
   *
   * @param left the left hand side embedding
   * @param right the right hand side embedding
   * @return the merged data represented as byte array
   */
  private byte[] mergePropertyData(Embedding left, Embedding right) {
    return ArrayUtils.addAll(left.getPropertyData(), right.getPropertyData());
  }

  /**
   * Merges the idListData columns of the left and right embeddings
   * All entries from both sides are kept.
   *
   * @param left the left hand side embedding
   * @param right the right hand side embedding
   * @return the merged data represented as byte array
   */
  private byte[] mergeIdListData(Embedding left, Embedding right) {
    return ArrayUtils.addAll(left.getIdListData(), right.getIdListData());
  }
}
