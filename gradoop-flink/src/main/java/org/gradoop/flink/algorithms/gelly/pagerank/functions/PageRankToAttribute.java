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
package org.gradoop.flink.algorithms.gelly.pagerank.functions;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Stores the page rank result from the left as a Property in in the right.
 */
public class PageRankToAttribute
  implements JoinFunction<PageRank.Result<GradoopId>, Vertex, Vertex> {

  /**
   * Property to store the page rank in.
   */
  private final String pageRankPropery;

  /**
   * Stores the page rank result as a Property.
   *
   * @param targetProperty Property name.
   */
  public PageRankToAttribute(String targetProperty) {
    this.pageRankPropery = targetProperty;
  }

  @Override
  public Vertex join(PageRank.Result result, Vertex vertex) {
    vertex.setProperty(pageRankPropery, PropertyValue.create(result.getPageRankScore().getValue()));
    return vertex;
  }
}
