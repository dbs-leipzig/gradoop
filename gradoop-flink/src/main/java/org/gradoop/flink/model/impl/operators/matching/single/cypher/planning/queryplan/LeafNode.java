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
