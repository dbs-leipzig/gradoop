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
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

import java.util.Arrays;

/**
 * Debug output for {@link EmbeddingWithTiePoint}.
 *
 * @param <K> key type
 */
public class PrintEmbeddingWithTiePoint<K> extends Printer<EmbeddingWithTiePoint<K>, K> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintEmbeddingWithTiePoint.class);

  /**
   * Constructor
   */
  public PrintEmbeddingWithTiePoint() {
    this(false, "");
  }

  /**
   * Constructor
   *
   * @param isIterative true, if used in iterative context
   */
  public PrintEmbeddingWithTiePoint(boolean isIterative) {
    this(isIterative, "");
  }

  /**
   * Constructor
   *
   * @param isIterative true, if used in iterative context
   * @param prefix      prefix for debug string
   */
  public PrintEmbeddingWithTiePoint(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  /**
   * Constructor
   *
   * @param iterationNumber true, if used in iterative context
   * @param prefix          prefix for debug string
   */
  public PrintEmbeddingWithTiePoint(int iterationNumber, String prefix) {
    super(iterationNumber, prefix);
  }

  @Override
  protected String getDebugString(EmbeddingWithTiePoint<K> embedding) {
    return String.format("(([%s],[%s]),%s)",
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEmbedding().getVertexMapping()), true), ','),
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEmbedding().getEdgeMapping()), false), ','),
      vertexMap.get(embedding.getTiePointId()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
