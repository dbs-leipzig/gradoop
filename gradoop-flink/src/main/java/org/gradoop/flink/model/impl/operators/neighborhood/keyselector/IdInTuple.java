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
package org.gradoop.flink.model.impl.operators.neighborhood.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns the id of an epgm element of a specified field of a given tuple.
 *
 * @param <T> type of the tuple
 */
public class IdInTuple<T extends Tuple> implements KeySelector<T, GradoopId> {

  /**
   * Field of the tuple which contains an epgm element to get the key from.
   */
  private int field;

  /**
   * Valued constructor.
   *
   * @param field field of the element to get the key from
   */
  public IdInTuple(int field) {
    this.field = field;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getKey(T tuple) throws Exception {
    return ((EPGMGraphElement) tuple.getField(field)).getId();
  }
}
