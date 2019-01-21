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
package org.gradoop.dataintegration.importer.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Represents a [gradoop id, key string] pair
 * f0 : gradoop id
 * f1 : key string
 */
public class IdKeyTuple extends Tuple2<GradoopId, String> {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * Empty Constructor.
   */
  public IdKeyTuple() {
  }

  /**
   * Creates an instance of {@link IdKeyTuple} to store id and key string.
   *
   * @param gradoopId gradoop id
   * @param key key value
   */
  public IdKeyTuple(GradoopId gradoopId, String key) {
    super(gradoopId, key);
  }

  /**
   * Get gradoop id.
   * @return gradoop id
   */
  public GradoopId getGradoopId() {
    return this.f0;
  }

  /**
   * Get key string.
   * @return key string
   */
  public String getKey() {
    return this.f1;
  }

  /**
   * Set gradoop id.
   * @param gradoopId gradoop id
   */
  public void setId(GradoopId gradoopId) {
    this.f0 = gradoopId;
  }

  /**
   * Set key string.
   * @param key key string
   */
  public void setKey(String key) {
    this.f1 = key;
  }
}
