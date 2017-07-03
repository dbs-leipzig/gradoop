
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Represents a leaf node in the query plan. Leaf nodes are different in terms of their input which
 * is a data set containing EPGM elements, i.e. {@link org.gradoop.common.model.impl.pojo.Vertex} or
 * {@link org.gradoop.common.model.impl.pojo.Edge}.
 */
public abstract class LeafNode extends PlanNode {
  /**
   * Sets the property columns in the specified meta data object according to the specified variable
   * and property keys.
   *
   * @param metaData meta data to update
   * @param variable variable to associate properties to
   * @param propertyKeys properties needed for filtering and projection
   * @return updated EmbeddingMetaData
   */
  protected EmbeddingMetaData setPropertyColumns(EmbeddingMetaData metaData, String variable,
    List<String> propertyKeys) {
    IntStream.range(0, propertyKeys.size())
      .forEach(i -> metaData.setPropertyColumn(variable, propertyKeys.get(i), i));
    return metaData;
  }
}
