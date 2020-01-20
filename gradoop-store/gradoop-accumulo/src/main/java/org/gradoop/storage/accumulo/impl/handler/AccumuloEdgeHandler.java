/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.storage.accumulo.impl.handler;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.storage.accumulo.impl.constants.AccumuloTables;

/**
 * Accumulo edge handler for row's read/write operator
 */
public class AccumuloEdgeHandler implements AccumuloRowHandler<EPGMEdge, Edge> {

  /**
   * Edge factory
   */
  private final EdgeFactory<EPGMEdge> factory;

  /**
   * Accumulo edge handler constructor
   *
   * @param factory edge factory
   */
  public AccumuloEdgeHandler(EdgeFactory<EPGMEdge> factory) {
    this.factory = factory;
  }

  @Override
  public Mutation writeRow(
    Mutation mutation,
    Edge record
  ) {
    mutation.put(AccumuloTables.KEY.LABEL, AccumuloTables.KEY.NONE, record.getLabel());
    mutation.put(AccumuloTables.KEY.SOURCE, AccumuloTables.KEY.NONE,
      record.getSourceId().toString());
    mutation.put(AccumuloTables.KEY.TARGET, AccumuloTables.KEY.NONE,
      record.getTargetId().toString());

    //encode properties value bytes as base64 string
    Iterable<String> keys = record.getPropertyKeys();
    if (keys != null) {
      keys.forEach(key -> mutation.put(
        /*cf*/AccumuloTables.KEY.PROPERTY,
        /*cq*/key,
        /*value*/new Value(record.getPropertyValue(key).getRawBytes())));
    }

    //write graph ids
    GradoopIdSet ids = record.getGraphIds();
    if (ids != null) {
      ids.forEach(
        id -> mutation.put(AccumuloTables.KEY.GRAPH, id.toString(), AccumuloTables.KEY.NONE));
    }
    return mutation;
  }

  @Override
  public EPGMEdge readRow(Edge origin) {
    return factory.initEdge(
      origin.getId(),
      origin.getLabel(),
      origin.getSourceId(),
      origin.getTargetId(),
      origin.getProperties(),
      origin.getGraphIds());
  }
}
