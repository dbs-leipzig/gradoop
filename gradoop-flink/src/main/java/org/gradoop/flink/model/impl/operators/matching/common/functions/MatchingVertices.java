/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.matching.ElementMatcher;

import java.util.Collection;

import static org.gradoop.common.util.GradoopConstants.DEFAULT_VERTEX_LABEL;

/**
 * Filter vertices based on their occurrence in the given GDL pattern.
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ReadFields("label;properties")
public class MatchingVertices<V extends Vertex> extends AbstractFilter<V> {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;
  /**
   * Query vertices to match against.
   */
  private transient Collection<org.s1ck.gdl.model.Vertex> queryVertices;
  /**
   * Create new filter.
   *
   * @param query GDL query string
   */
  public MatchingVertices(final String query) {
    super(query);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryVertices = getQueryHandler().getVertices();
  }

  @Override
  public boolean filter(V v) throws Exception {
    return ElementMatcher.matchAll(v, queryVertices, DEFAULT_VERTEX_LABEL);
  }
}
