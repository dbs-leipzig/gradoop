/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.flink.io.reader.parsers.amazon.vertices;

import org.gradoop.flink.io.reader.parsers.inputfilerepresentations.Vertexable;

/**
 * Defines a reivewer reviewing an element
 */
public class Reviewer extends Vertexable<String> implements Comparable<Reviewer> {

  /**
   * Vertex id
   */
  private String rewId;

  /**
   * Vertex name
   */
  private String rewName;

  /**
   * Default empty constructor
   */
  public Reviewer() {
    this.rewId = "R";
    this.rewName = "";
  }

  /**
   * Setting…
   * @param rewId the vertex id
   */
  public void setRewId(String rewId) {
    this.rewId = rewId;
  }

  /**
   * Setting…
   * @param rewName the user's name
   */
  public void setRewName(String rewName) {
    this.rewName = rewName;
  }

  @Override
  public int compareTo(Reviewer o) {
    return rewId.compareTo(o.rewId);
  }

  @Override
  public String getId() {
    return rewId;
  }

  @Override
  public String getLabel() {
    return "User";
  }

  @Override
  public void updateByParse(String toParse) {
    throw new RuntimeException("Error: this method should never be invoked");
  }
}
