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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos;

import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData.EntryType;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.binary.JoinEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.leaf.FilterAndProjectEdgesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.leaf.FilterAndProjectVerticesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.binary.ExpandEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.unary.ProjectEmbeddingsNode;

import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class EmbeddingMetaDataFactory {

  /**
   * Creates the resulting {@link EmbeddingMetaData} for {@link FilterAndProjectVerticesNode},
   *
   * @param vertexVariable variable of the vertex
   * @param propertyKeys properties needed for filtering and projection
   * @return meta data describing the output of the corresponding plan node
   */
  public static EmbeddingMetaData forFilterAndProjectVertices(
    String vertexVariable, List<String> propertyKeys) {

    EmbeddingMetaData embeddingMetaData = new EmbeddingMetaData();
    embeddingMetaData.setEntryColumn(vertexVariable, EntryType.VERTEX, 0);
    embeddingMetaData = setPropertyColumns(embeddingMetaData, vertexVariable, propertyKeys);

    return embeddingMetaData;
  }

  /**
   * Creates the resulting {@link EmbeddingMetaData} for {@link FilterAndProjectEdgesNode}.
   *
   * @param sourceVariable variable of the source vertex
   * @param edgeVariable variable of the edge
   * @param targetVariable variable of the target vertex
   * @param propertyKeys properties needed for filtering and projection
   * @return meta data describing the output of the corresponding plan node
   */
  public static EmbeddingMetaData forFilterAndProjectEdges(
    String sourceVariable, String edgeVariable, String targetVariable, List<String> propertyKeys) {

    EmbeddingMetaData embeddingMetaData = new EmbeddingMetaData();
    embeddingMetaData.setEntryColumn(sourceVariable, EntryType.VERTEX, 0);
    embeddingMetaData.setEntryColumn(edgeVariable, EntryType.EDGE, 1);
    embeddingMetaData.setEntryColumn(targetVariable, EntryType.VERTEX, 2);

    embeddingMetaData = setPropertyColumns(embeddingMetaData, edgeVariable, propertyKeys);

    return embeddingMetaData;
  }

  /**
   * Creates the resulting {@link EmbeddingMetaData} for {@link FilterEmbedding}.
   *
   * @param inputMetaData embedding meta data of the input embedding
   * @return meta data describing the output of the corresponding plan node
   */
  public static EmbeddingMetaData forFilterEmbeddings(EmbeddingMetaData inputMetaData) {
    return new EmbeddingMetaData(inputMetaData);
  }

  /**
   * Creates the resulting {@link EmbeddingMetaData} for {@link ProjectEmbeddingsNode}.
   *
   * @param inputMetaData embedding meta data of the input embedding
   * @param propertyKeys properties to project
   * @return meta data describing the output of the corresponding plan node
   */
  public static EmbeddingMetaData forProjectEmbeddings(EmbeddingMetaData inputMetaData,
    List<Pair<String, String>> propertyKeys) {

    propertyKeys.sort(Comparator.comparingInt(key ->
      inputMetaData.getPropertyColumn(key.getLeft(), key.getRight())));

    EmbeddingMetaData embeddingMetaData = new EmbeddingMetaData();

    inputMetaData.getVariables().forEach(var -> embeddingMetaData.setEntryColumn(
      var, inputMetaData.getEntryType(var), inputMetaData.getEntryColumn(var)));

    IntStream.range(0, propertyKeys.size()).forEach(i -> embeddingMetaData.setPropertyColumn(
      propertyKeys.get(i).getLeft(), propertyKeys.get(i).getRight(), i));

    return embeddingMetaData;
  }

  /**
   * Creates the resulting {@link EmbeddingMetaData} for {@link JoinEmbeddingsNode}.
   *
   * @param leftInputMetaData meta data for the left join input
   * @param rightInputMetaData meta data for the right join input
   * @param joinVariables variables to join embeddings on
   * @return meta data describing the output of the corresponding plan node
   */
  public static EmbeddingMetaData forJoinEmbeddings(EmbeddingMetaData leftInputMetaData,
    EmbeddingMetaData rightInputMetaData, List<String> joinVariables) {

    EmbeddingMetaData embeddingMetaData = new EmbeddingMetaData(leftInputMetaData);

    int entryCount = leftInputMetaData.getEntryCount();

    // append the non-join entry mappings from the right to the left side
    for (String var : rightInputMetaData.getVariables()) {
      if (!joinVariables.contains(var)) {
        embeddingMetaData.setEntryColumn(var, rightInputMetaData.getEntryType(var), entryCount++);
      }
    }

    // append all property mappings from the right to the left side
    int propertyCount = leftInputMetaData.getPropertyCount();
    for (String var : rightInputMetaData.getVariables()) {
      for (String key : rightInputMetaData.getPropertyKeys(var)) {
        embeddingMetaData.setPropertyColumn(var, key, propertyCount++);
      }
    }
    return embeddingMetaData;
  }

  /**
   * Creates the resulting {@link EmbeddingMetaData} for {@link ExpandEmbeddingsNode}.
   *
   * @param inputMetaData meta data associated with the expand input
   * @param pathVariable variable to expand embeddings from
   * @return meta data describing the output of the corresponding plan node
   */
  public static EmbeddingMetaData forExpandEmbeddings(EmbeddingMetaData inputMetaData,
    String pathVariable, String endVariable) {
    EmbeddingMetaData embedding = new EmbeddingMetaData(inputMetaData);

    embedding.setEntryColumn(pathVariable, EntryType.PATH, inputMetaData.getEntryCount());

    if (!inputMetaData.containsEntryColumn(endVariable)) {
      embedding.setEntryColumn(endVariable, EntryType.VERTEX, inputMetaData.getEntryCount() + 1);
    }

    return embedding;
  }


  /**
   * Sets the property columns in the specified meta data object according to the specified variable
   * and property keys.
   *
   * @param metaData meta data to update
   * @param variable variable to associate properties to
   * @param propertyKeys properties needed for filtering and projection
   * @return updated EmbeddingMetaData
   */
  private static EmbeddingMetaData setPropertyColumns(EmbeddingMetaData metaData, String variable,
    List<String> propertyKeys) {
    IntStream.range(0, propertyKeys.size())
      .forEach(i -> metaData.setPropertyColumn(variable, propertyKeys.get(i), i));
    return metaData;
  }
}
