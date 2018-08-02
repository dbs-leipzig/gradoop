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

package org.gradoop.storage.impl.accumulo.functions;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.api.EPGMGraphOutput;
import org.gradoop.storage.common.iterator.ClosableIterator;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.config.GradoopAccumuloConfig;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Create a new element mapping function
 *
 * @param <E> element to be read
 */
public class ElementMapFunction<E extends Element> implements MapPartitionFunction<GradoopId, E> {

  /**
   * Element type
   */
  private final Class<E> elementType;

  /**
   * Should result contains edge income
   */
  private final GradoopAccumuloConfig config;

  /**
   * Element filter predicate
   */
  private final AccumuloElementFilter<E> filter;

  /**
   * Create a new element map function
   *
   * @param elementType element type
   * @param config accumulo configuration
   * @param filter accumulo element filter
   */
  public ElementMapFunction(
    @Nonnull Class<E> elementType,
    @Nonnull GradoopAccumuloConfig config,
    @Nullable AccumuloElementFilter<E> filter
  ) {
    //noinspection unchecked, get genetic type of class E
    this.elementType = elementType;
    this.config = config;
    this.filter = filter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void mapPartition(
    @Nonnull Iterable<GradoopId> ids,
    @Nonnull Collector<E> collector
  ) throws Exception {
    //create a epgm store in each partition
    AccumuloEPGMStore store = new AccumuloEPGMStore(config);
    Iterator<GradoopId> iterator = ids.iterator();

    do {
      List<GradoopId> split = new ArrayList<>();
      for (int i = 0; i < EPGMGraphOutput.DEFAULT_CACHE_SIZE && iterator.hasNext(); i++) {
        split.add(iterator.next());
      }

      ClosableIterator<E> resultIterator;
      if (elementType == GraphHead.class) {
        resultIterator = (ClosableIterator<E>)
          getGraphHeads(split, store, (AccumuloElementFilter<GraphHead>) filter);

      } else if (elementType == Vertex.class) {
        resultIterator = (ClosableIterator<E>)
          getVertices(split, store, (AccumuloElementFilter<Vertex>) filter);

      } else if (elementType == Edge.class) {
        resultIterator = (ClosableIterator<E>)
          getEdges(split, store, (AccumuloElementFilter<Edge>) filter);

      } else {
        throw new IllegalStateException(String.format("illegal element type %s", elementType));
      }

      resultIterator.readRemainsAndClose().forEach(collector::collect);

    } while (iterator.hasNext());
  }

  /**
   * Get GraphHead within id split and predicate filter
   *
   * @param split id split
   * @param store store instance
   * @param graphHeadFilter graph head filter
   * @return result iterator
   * @throws IOException if read error
   */
  private ClosableIterator<GraphHead> getGraphHeads(
    @Nonnull List<GradoopId> split,
    @Nonnull AccumuloEPGMStore store,
    @Nullable AccumuloElementFilter<GraphHead> graphHeadFilter
  ) throws IOException {
    return store.getGraphSpace(graphHeadFilter == null ?
      Query.elements().fromSets(GradoopIdSet.fromExisting(split)).noFilter() :
      Query.elements().fromSets(GradoopIdSet.fromExisting(split)).where(graphHeadFilter));
  }

  /**
   * Get Vertex within id split and predicate filter
   *
   * @param split id split
   * @param store store instance
   * @param vertexFilter vertex filter
   * @return result iterator
   * @throws IOException if read error
   */
  private ClosableIterator<Vertex> getVertices(
    @Nonnull List<GradoopId> split,
    @Nonnull AccumuloEPGMStore store,
    @Nullable AccumuloElementFilter<Vertex> vertexFilter
  ) throws IOException {
    return store.getVertexSpace(vertexFilter == null ?
      Query.elements().fromSets(GradoopIdSet.fromExisting(split)).noFilter() :
      Query.elements().fromSets(GradoopIdSet.fromExisting(split)).where(vertexFilter));
  }

  /**
   * Get Edge within id split and predicate filter
   *
   * @param split id split
   * @param store store instance
   * @param edgeFilter edge filter
   * @return result iterator
   * @throws IOException if read error
   */
  private ClosableIterator<Edge> getEdges(
    @Nonnull List<GradoopId> split,
    @Nonnull AccumuloEPGMStore store,
    @Nullable AccumuloElementFilter<Edge> edgeFilter
  ) throws IOException {
    return store.getEdgeSpace(edgeFilter == null ?
      Query.elements().fromSets(GradoopIdSet.fromExisting(split)).noFilter() :
      Query.elements().fromSets(GradoopIdSet.fromExisting(split)).where(edgeFilter));
  }

}
