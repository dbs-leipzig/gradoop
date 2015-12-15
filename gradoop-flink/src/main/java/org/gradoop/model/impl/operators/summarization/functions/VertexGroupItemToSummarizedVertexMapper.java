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

package org.gradoop.model.impl.operators.summarization.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.operators.summarization.Summarization;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;

/**
 * Creates a new vertex representing a vertex group. The vertex stores the
 * group label, the group property value and the number of vertices in the
 * group.
 *
 * @param <V> EPGM vertex type
 */
public class VertexGroupItemToSummarizedVertexMapper<V extends EPGMVertex>
  implements MapFunction<VertexGroupItem, V>, ResultTypeQueryable<V> {

  /**
   * Vertex data factory.
   */
  private final EPGMVertexFactory<V> vertexFactory;
  /**
   * Vertex property key used for grouping.
   */
  private final String groupPropertyKey;
  /**
   * True, if the vertex label shall be considered.
   */
  private final boolean useLabel;
  /**
   * True, if the vertex property shall be considered.
   */
  private final boolean useProperty;

  /**
   * Creates map function.
   *
   * @param vertexFactory vertex data factory
   * @param groupPropertyKey  vertex property key for grouping
   * @param useLabel          true, if vertex label shall be considered
   */
  public VertexGroupItemToSummarizedVertexMapper(
    EPGMVertexFactory<V> vertexFactory, String groupPropertyKey,
    boolean useLabel) {
    this.vertexFactory = vertexFactory;
    this.groupPropertyKey = groupPropertyKey;
    this.useLabel = useLabel;
    useProperty = groupPropertyKey != null && !"".equals(groupPropertyKey);
  }

  /**
   * Creates a {@link EPGMVertex} object from the given {@link
   * VertexGroupItem} and returns a new {@link Vertex}.
   *
   * @param vertexGroupItem vertex group item
   * @return vertex including new vertex data
   * @throws Exception
   */
  @Override
  public V map(VertexGroupItem vertexGroupItem) throws
    Exception {
    V summarizedVertexData = vertexFactory.initVertex(
      vertexGroupItem.getGroupRepresentativeVertexId());
    if (useLabel) {
      summarizedVertexData.setLabel(vertexGroupItem.getGroupLabel());
    }
    if (useProperty) {
      summarizedVertexData.setProperty(
        groupPropertyKey, vertexGroupItem.getGroupPropertyValue());
    }
    summarizedVertexData.setProperty(
      Summarization.COUNT_PROPERTY_KEY,
      vertexGroupItem.getGroupCount());

    return summarizedVertexData;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<V> getProducedType() {
    return (TypeInformation<V>)
      TypeExtractor.createTypeInfo(vertexFactory.getType());
  }
}
