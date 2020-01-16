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
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.storage.accumulo.impl.constants.AccumuloTables;

/**
 * accumulo graph head handler for row's read/write operator
 */
public class AccumuloGraphHandler implements AccumuloRowHandler<EPGMGraphHead, GraphHead> {

  /**
   * graph head factory
   */
  private final GraphHeadFactory<EPGMGraphHead> factory;

  /**
   * graph head handler constructor
   *
   * @param factory graph head factory
   */
  public AccumuloGraphHandler(GraphHeadFactory<EPGMGraphHead> factory) {
    this.factory = factory;
  }

  @Override
  public Mutation writeRow(
    Mutation mutation,
    GraphHead record
  ) {
    mutation.put(AccumuloTables.KEY.LABEL, AccumuloTables.KEY.NONE, record.getLabel());

    Iterable<String> keys = record.getPropertyKeys();
    if (keys != null) {
      keys.forEach(key -> mutation.put(
        /*cf*/AccumuloTables.KEY.PROPERTY,
        /*cq*/key,
        /*value*/new Value(record.getPropertyValue(key).getRawBytes())));
    }
    return mutation;
  }

  @Override
  public EPGMGraphHead readRow(GraphHead origin) {
    return factory.initGraphHead(
      /*edge id*/origin.getId(),
      /*label*/origin.getLabel(),
      /*properties*/origin.getProperties());
  }

}
