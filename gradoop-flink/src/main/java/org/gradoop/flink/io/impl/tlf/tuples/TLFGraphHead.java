/**
 * Copyright © 2014 Gradoop (University of Leipzig - Database Research Group)
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
package org.gradoop.flink.io.impl.tlf.tuples;

import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Represents a graph head used in a graph generation from TLF-files.
 */
public class TLFGraphHead extends Tuple1<Long> {

  /**
   * default constructor
   */
  public TLFGraphHead() {
  }

  /**
   * valued constructor
   * @param id graph head id
   */
  public TLFGraphHead(Long id) {
    super(id);
  }

  public Long getId() {
    return this.f0;
  }

  public void setId(long id) {
    this.f0 = id;
  }

}
