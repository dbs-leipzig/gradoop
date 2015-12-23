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
import org.gradoop.model.impl.operators.summarization.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.operators.summarization.tuples.EdgeGroupItem;
import org.gradoop.model.impl.properties.PropertyValueList;
import org.gradoop.util.GConstants;

import java.util.List;

/**
 * Creates a summarized edge from a group of edges including an edge
 * grouping value.
 *
 * @param <E> EPGM edge type
 */
public class BuildSummarizedEdge<E extends EPGMEdge>
  extends BuildBase
  implements GroupReduceFunction<EdgeGroupItem, E>, ResultTypeQueryable<E> {

  /**
   * Edge factory.
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Creates group reducer
   *
   * @param groupPropertyKeys edge property keys
   * @param useLabel          use edge label
   * @param valueAggregator   aggregate function for edge values
   * @param edgeFactory       edge factory
   */
  public BuildSummarizedEdge(List<String> groupPropertyKeys,
    boolean useLabel,
    PropertyValueAggregator valueAggregator,
    EPGMEdgeFactory<E> edgeFactory) {
    super(groupPropertyKeys, useLabel, valueAggregator);
    this.edgeFactory = edgeFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reduce(Iterable<EdgeGroupItem> edgeGroupItems,
    Collector<E> collector) throws Exception {

    GradoopId newSourceVertexId           = null;
    GradoopId newTargetVertexId           = null;
    String edgeLabel                      = GConstants.DEFAULT_EDGE_LABEL;
    PropertyValueList groupPropertyValues = null;

    boolean firstElement                  = false;

    for (EdgeGroupItem e : edgeGroupItems) {
      if (!firstElement) {
        newSourceVertexId = e.getSourceId();
        newTargetVertexId = e.getTargetId();
        if (useLabel()) {
          edgeLabel = e.getGroupLabel();
        }
        groupPropertyValues = e.getGroupPropertyValues();
        firstElement = true;
      }

      if (doAggregate()) {
        aggregate(e.getGroupAggregate());
      } else {
        break;
      }
    }

    E sumEdge = edgeFactory.createEdge(
      edgeLabel, newSourceVertexId, newTargetVertexId);

    setGroupProperties(sumEdge, groupPropertyValues);
    setAggregate(sumEdge);
    resetAggregator();

    collector.collect(sumEdge);
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
