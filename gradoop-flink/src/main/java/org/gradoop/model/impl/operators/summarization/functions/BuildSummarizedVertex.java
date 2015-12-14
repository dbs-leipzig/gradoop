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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
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
import org.gradoop.model.impl.properties.PropertyValue;

import java.util.Iterator;
import java.util.List;

/**
 * Creates a new vertex representing a vertex group. The vertex stores the
 * group label, the group property value and the number of vertices in the
 * group.
 *
 * @param <V> EPGM vertex type
 */
public class BuildSummarizedVertex<V extends EPGMVertex>
  implements MapFunction<VertexGroupItem, V>, ResultTypeQueryable<V> {


  /**
   * Vertex property keys used for grouping.
   */
  private final List<String> groupPropertyKeys;

  /**
   * True, if the vertex label shall be considered.
   */
  private final boolean useLabel;

  /**
   * Vertex factory.
   */
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * Creates map function.
   *
   * @param groupPropertyKeys vertex property key for grouping
   * @param useLabel          true, if vertex label shall be considered
   * @param vertexFactory     vertex factory
   */
  public BuildSummarizedVertex(List<String> groupPropertyKeys, boolean useLabel,
    EPGMVertexFactory<V> vertexFactory) {
    this.groupPropertyKeys = groupPropertyKeys;
    this.useLabel = useLabel;
    this.vertexFactory = vertexFactory;
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
    V summarizedVertex = vertexFactory.initVertex(
      vertexGroupItem.getGroupRepresentative());

    appendLabel(vertexGroupItem, summarizedVertex);
    appendProperties(vertexGroupItem, summarizedVertex);
    appendAggregateValues(vertexGroupItem, summarizedVertex);

    return summarizedVertex;
  }

  /**
   * Appends the label if necessary.
   *
   * @param vertexGroupItem   vertex group item
   * @param summarizedVertex  summarized vertex
   */
  private void appendLabel(VertexGroupItem vertexGroupItem,
    V summarizedVertex) {
    if (useLabel) {
      summarizedVertex.setLabel(vertexGroupItem.getGroupLabel());
    }
  }

  /**
   * Appends aggregate values if necessary.
   *
   * @param vertexGroupItem   vertex group item
   * @param summarizedVertex  summarized vertex
   */
  private void appendAggregateValues(VertexGroupItem vertexGroupItem,
    V summarizedVertex) {
    summarizedVertex.setProperty(
      Summarization.COUNT_PROPERTY_KEY,
      vertexGroupItem.getGroupCount());
  }

  /**
   * Appends properties if necessary.
   *
   * @param vertexGroupItem   vertex group item
   * @param summarizedVertex  summarized vertex
   */
  private void appendProperties(VertexGroupItem vertexGroupItem,
    V summarizedVertex) {

    Iterator<String> keyIterator = groupPropertyKeys.iterator();
    Iterator<PropertyValue> valueIterator =
      vertexGroupItem.getGroupPropertyValues().iterator();

    while (keyIterator.hasNext() && valueIterator.hasNext()) {
      summarizedVertex.setProperty(keyIterator.next(), valueIterator.next());
    }
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
