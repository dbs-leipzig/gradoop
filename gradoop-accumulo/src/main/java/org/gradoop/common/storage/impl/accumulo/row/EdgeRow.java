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

package org.gradoop.common.storage.impl.accumulo.row;

import java.util.ArrayList;
import java.util.List;

/**
 * accumulo edge rpc model wrapper
 */
public class EdgeRow extends ElementRow {

  /**
   * edge source
   */
  private String source;

  /**
   * edge target
   */
  private String target;

  /**
   * graph belonging id set
   */
  private List<String> graphs = new ArrayList<>();

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getTarget() {
    return target;
  }

  public void setTarget(String target) {
    this.target = target;
  }

  public List<String> getGraphs() {
    return graphs;
  }

  public void setGraph(List<String> graphs) {
    this.graphs = graphs;
  }

}
