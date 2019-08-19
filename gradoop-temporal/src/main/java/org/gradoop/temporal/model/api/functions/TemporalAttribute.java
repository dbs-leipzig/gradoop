/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.api.functions;

/**
 * Attributes accessible on {@link org.gradoop.temporal.model.impl.pojo.TemporalElement}s.
 * Those attributes are expected to be time intervals.
 */
public enum TemporalAttribute {
  /**
   * The transaction time of a temporal element, i.e. the time interval in which the element is
   * considered part of the graph database. This interval should be maintained by the system.
   */
  TRANSACTION_TIME,
  /**
   * The valid time of a temporal element, i.e. the time in which the element's data is considered valid in
   * a certain context. Validity is therefore depending on the data itself and maintained by the user.
   */
  VALID_TIME;

  /**
   * Fields accessible in a time interval.
   */
  public enum Field {
    /**
     * The start of the interval.
     */
    FROM,
    /**
     * The end of the interval.
     */
    TO
  }
}
