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
package org.gradoop.flink.model.impl.operators.tostring.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (graphId, sourceId, targetId, sourceLabel, edgeLabel, targetLabel)
 */
public class EdgeString extends Tuple6<GradoopId, GradoopId, GradoopId,
  String, String, String> {

  /**
   * default constructor
   */
  public EdgeString() {
  }

  /**
   * constructor with field values
   * @param graphId graph id
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param label edge label
   */
  public EdgeString(
    GradoopId graphId, GradoopId sourceId, GradoopId targetId, String label) {

    this.f0 = graphId;
    this.f1 = sourceId;
    this.f2 = targetId;
    this.f3 = "";
    this.f4 = label;
    this.f5 = "";
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public GradoopId getSourceId() {
    return this.f1;
  }

  public GradoopId getTargetId() {
    return this.f2;
  }

  public String getSourceLabel() {
    return this.f3;
  }

  public void setSourceLabel(String sourceLabel) {
    this.f3 = sourceLabel;
  }

  public String getEdgeLabel() {
    return this.f4;
  }

  public void setEdgeLabel(String label) {
    this.f4 = label;
  }

  public String getTargetLabel() {
    return this.f5;
  }

  public void setTargetLabel(String targetLabel) {
    this.f5 = targetLabel;
  }

  public void setSourceId(GradoopId sourceId) {
    this.f1 = sourceId;
  }

  public void setTargetId(GradoopId targetId) {
    this.f2 = targetId;
  }
}
