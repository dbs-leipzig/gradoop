/**
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

package org.gradoop.common.storage.impl.accumulo.handler;

import org.apache.accumulo.core.data.Mutation;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.row.VertexRow;

import java.util.Base64;
import java.util.stream.Collectors;

/**
 * accumulo vertex handler for row's read/write operator
 *
 * @param <V> vertex element as reading result
 */
public class AccumuloVertexHandler<V extends EPGMVertex> implements
  AccumuloRowHandler<V, EPGMVertex, VertexRow> {

  /**
   * vertex factory
   */
  private final EPGMVertexFactory<V> factory;

  /**
   * vertex handler factory constructor
   *
   * @param factory vertex factory
   */
  public AccumuloVertexHandler(EPGMVertexFactory<V> factory) {
    this.factory = factory;
  }

  @Override
  public Mutation writeRow(
    Mutation mutation,
    EPGMVertex record
  ) {
    mutation.put(AccumuloTables.KEY.LABEL, AccumuloTables.KEY.NONE, record.getLabel());

    //encode properties value bytes as base64 string
    Iterable<String> keys = record.getPropertyKeys();
    if (keys != null) {
      keys.forEach(key -> mutation.put(
        /*cf*/AccumuloTables.KEY.PROPERTY,
        /*cq*/key,
        /*value*/Base64.getEncoder().encodeToString(record.getPropertyValue(key).getRawBytes())));
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
  public V readRow(
    GradoopId id,
    VertexRow row
  ) {
    Properties properties = new Properties();
    row.getProperties().forEach((k, v) -> {
      PropertyValue value = PropertyValue.fromRawBytes(Base64.getDecoder().decode(v));

      //properties values where transport as base64 encoded string instead of byte arrays
      properties.set(k, value);
    });

    /*graph sets*/
    GradoopIdSet graphSets = GradoopIdSet.fromExisting(
      row.getGraphs().stream().map(GradoopId::fromString).collect(Collectors.toList()));

    return factory.initVertex(
      /*edge id*/id,
      /*label*/row.getLabel(),
      /*properties*/properties,
      /*graph sets*/graphSets);
  }

}
