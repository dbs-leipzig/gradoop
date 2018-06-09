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
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.row.GraphHeadRow;

import java.util.Base64;

/**
 * accumulo graph head handler for row's read/write operator
 *
 * @param <G> graph head as reading result
 */
public class AccumuloGraphHandler<G extends EPGMGraphHead> implements
  AccumuloRowHandler<G, EPGMGraphHead, GraphHeadRow> {

  /**
   * graph head factory
   */
  private final EPGMGraphHeadFactory<G> factory;

  /**
   * graph head handler constructor
   *
   * @param factory graph head factory
   */
  public AccumuloGraphHandler(EPGMGraphHeadFactory<G> factory) {
    this.factory = factory;
  }

  @Override
  public Mutation writeRow(
    Mutation mutation,
    EPGMGraphHead record
  ) {
    mutation.put(AccumuloTables.KEY.LABEL, AccumuloTables.KEY.NONE, record.getLabel());

    Iterable<String> keys = record.getPropertyKeys();
    if (keys != null) {
      keys.forEach(key -> mutation.put(
        /*cf*/AccumuloTables.KEY.PROPERTY,
        /*cq*/key,
        /*value*/Base64.getEncoder().encodeToString(record.getPropertyValue(key).getRawBytes())));
    }
    return mutation;
  }

  @Override
  public G readRow(
    GradoopId id,
    GraphHeadRow row
  ) {
    Properties properties = new Properties();
    row.getProperties().forEach((k, v) -> {
      PropertyValue value = PropertyValue.fromRawBytes(Base64.getDecoder().decode(v));

      //properties values where transport as base64 encoded string instead of byte arrays
      properties.set(k, value);
    });

    //properties values where transport as base64 encoded string instead of byte arrays
    return factory.initGraphHead(
      /*edge id*/id,
      /*label*/row.getLabel(),
      /*properties*/properties);
  }


}
