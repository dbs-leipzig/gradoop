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

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Represents a vertex used in a graph generation from TLF-files.
 */
public class TLFVertex extends Tuple2<Integer, String> {

  /**
   * Symbol identifying a line to represent a vertex.
   */
  public static final String SYMBOL = "v";

  /**
   * default constructor
   */
  public TLFVertex() {
  }

  /**
   * valued constructor
   * @param id vertex id
   * @param label vertex label
   */
  public TLFVertex(Integer id, String label) {
    super(id, label);
  }

  public Integer getId() {
    return this.f0;
  }

  public void setId(Integer id) {
    this.f0 = id;
  }

  public String getLabel() {
    return this.f1;
  }

  public void setLabel(String label) {
    this.f1 = label;
  }
}
