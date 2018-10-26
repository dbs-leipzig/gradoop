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
package org.gradoop.flink.model.api.functions.timeextractors;

import org.gradoop.common.model.impl.pojo.Element;

import java.io.Serializable;

/**
 * Map function to extract temporal information from an EPGM element to create a TPGM element
 * with a time interval as the elements validity.
 *
 * @param <E> the EPGM element type
 */
public interface TimeIntervalExtractor<E extends Element> extends TimestampExtractor<E>,
  Serializable {
  /**
   * Function to extract the end of the elements validity as unix timestamp in milliseconds.
   *
   * @param element the element to extract the temporal information from
   * @return the end of the elements validity as unix timestamp in milliseconds
   */
  Long getValidTo(E element);
}
