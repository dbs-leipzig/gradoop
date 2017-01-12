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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.JoinEmbeddings;

import java.util.HashSet;
import java.util.List;
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
   * Creates a new UDF instance.
   *
   * @param joinColumnsRight join columns of the right side
   * @param distinctVertexColumnsLeft distinct vertex columns of the left embedding
   * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
   * @param distinctEdgeColumnsLeft distinct edge columns of the left embedding
   * @param distinctEdgeColumnsRight distinct edge columns of the right embedding
   */
  public MergeEmbeddings(List<Integer> joinColumnsRight,
    List<Integer> distinctVertexColumnsLeft,
    List<Integer> distinctVertexColumnsRight,
    List<Integer> distinctEdgeColumnsLeft,
    List<Integer> distinctEdgeColumnsRight) {
    this.joinColumnsRight           = joinColumnsRight;
    this.distinctVertexColumnsLeft  = distinctVertexColumnsLeft;
    this.distinctVertexColumnsRight = distinctVertexColumnsRight;
    this.distinctEdgeColumnsLeft    = distinctEdgeColumnsLeft;
    this.distinctEdgeColumnsRight   = distinctEdgeColumnsRight;
  }

  @Override
  public void join(Embedding left, Embedding right, Collector<Embedding> out)
      throws Exception {

    if (isDistinct(distinctVertexColumnsLeft, distinctVertexColumnsRight, left, right) &&
      isDistinct(distinctEdgeColumnsLeft, distinctEdgeColumnsRight, left, right)) {
      out.collect(new Embedding(
        mergeIdData(left, right), mergePropertyData(left, right), mergeIdListData(left, right)
      ));
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
      (joinColumnsRight.size() * (GradoopId.ID_SIZE + 1))
    ];

    int offset = left.getIdData().length;
    System.arraycopy(left.getIdData(), 0, newIdData, 0, offset);

    for (int i = 0; i < right.size(); i++) {
      if (joinColumnsRight.contains(i)) {
        continue;
      }

      System.arraycopy(right.getRawIdEntry(i), 0, newIdData, offset, Embedding
        .ID_ENTRY_SIZE);
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
    int leftLenght = left.getPropertyData().length;
    int rightLenght = right.getPropertyData().length;

    byte[] newPropertyData = new byte[leftLenght + rightLenght];

    System.arraycopy(left.getPropertyData(), 0, newPropertyData, 0, leftLenght);
    System.arraycopy(right.getPropertyData(), 0, newPropertyData, leftLenght, rightLenght);

    return newPropertyData;
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
    int leftLenght = left.getIdListData().length;
    int rightLenght = right.getIdListData().length;

    byte[] newIdListData = new byte[leftLenght + rightLenght];

    System.arraycopy(left.getIdListData(), 0, newIdListData, 0, leftLenght);
    System.arraycopy(right.getIdListData(), 0, newIdListData, leftLenght, rightLenght);

    return newIdListData;
  }
}
