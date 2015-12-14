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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.summarization.Summarization;
import org.gradoop.model.impl.operators.summarization.tuples.EdgeGroupItem;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.model.impl.properties.PropertyValueList;
import org.gradoop.util.GConstants;

import java.util.Iterator;
import java.util.List;

/**
 * Creates a summarized edge from a group of edges including an edge
 * grouping value.
 *
 * @param <E> EPGM edge type
 */
public class BuildSummarizedEdge<E extends EPGMEdge>
  implements GroupReduceFunction<EdgeGroupItem, E>, ResultTypeQueryable<E> {

  /**
   * Edge property keys used for grouping.
   */
  private final List<String> groupPropertyKeys;
  /**
   * True, if label shall be considered.
   */
  private boolean useLabel;
  /**
   * Edge factory.
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Creates group reducer
   *
   * @param groupPropertyKeys edge property keys
   * @param useLabel          use edge label
   * @param edgeFactory       edge factory
   */
  public BuildSummarizedEdge(List<String> groupPropertyKeys, boolean useLabel,
    EPGMEdgeFactory<E> edgeFactory) {
    this.groupPropertyKeys = groupPropertyKeys;
    this.useLabel = useLabel;
    this.edgeFactory = edgeFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reduce(Iterable<EdgeGroupItem> edgeGroupItems,
    Collector<E> collector) throws Exception {

    int edgeCount = 0;
    boolean initialized = false;

    GradoopId newSourceVertexId = null;
    GradoopId newTargetVertexId = null;
    String edgeLabel = GConstants.DEFAULT_EDGE_LABEL;
    PropertyValueList groupPropertyValues = null;

    for (EdgeGroupItem e : edgeGroupItems) {
      edgeCount++;
      if (!initialized) {
        newSourceVertexId = e.getSourceId();
        newTargetVertexId = e.getTargetId();
        if (useLabel) {
          edgeLabel = e.getGroupLabel();
        }
        groupPropertyValues = e.getGroupPropertyValues();
        initialized = true;
      }
    }

    E e = edgeFactory.createEdge(
      edgeLabel, newSourceVertexId, newTargetVertexId);
    // properties
    appendProperties(groupPropertyValues, e);
    // aggregate functions
    appendAggregateValues(edgeCount, e);

    collector.collect(e);
  }

  /**
   * Append aggregate values to the new edge.
   *
   * @param edgeCount number of edges represented by that edge
   * @param e         summarized edge
   */
  private void appendAggregateValues(int edgeCount, E e) {
    e.setProperty(Summarization.COUNT_PROPERTY_KEY, edgeCount);
  }

  /**
   * Append group property values to the new edge.
   *
   *
   * @param groupPropertyValues property values for that group
   * @param e                   summarized edge
   */
  private void appendProperties(PropertyValueList groupPropertyValues, E e) {
    Iterator<String> keyIterator = groupPropertyKeys.iterator();
    Iterator<PropertyValue> valueIterator = groupPropertyValues.iterator();

    while (keyIterator.hasNext() && valueIterator.hasNext()) {
      e.setProperty(keyIterator.next(), valueIterator.next());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public TypeInformation<E> getProducedType() {
    return (TypeInformation<E>)
      TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
