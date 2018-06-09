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
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.impl.accumulo.row.ElementRow;

/**
 * accumulo row handler
 *
 * @param <E> row to decode from remote
 * @param <W> row to store
 * @param <R> row to
 */
public interface AccumuloRowHandler<E extends EPGMElement, W extends EPGMElement, R extends
  ElementRow> {

  /**
   * write element to store
   *
   * @param mutation new row mutation
   * @param record EPGMElement to be write
   * @return row mutation after writer
   */
  Mutation writeRow(Mutation mutation, W record);

  /**
   * read element from iterator result
   *
   * @param id element id by row key
   * @param row element row
   * @return EPGM element
   */
  E readRow(GradoopId id, R row);

}
