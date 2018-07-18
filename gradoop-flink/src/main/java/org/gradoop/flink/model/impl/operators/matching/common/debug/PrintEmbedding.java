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
package org.gradoop.flink.model.impl.operators.matching.common.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;

import java.util.Arrays;

/**
 * Debug output for {@link Embedding}.
 *
 * @param <K> key type
 */
public class PrintEmbedding<K extends Comparable<K>> extends Printer<Embedding<K>, K> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintEmbedding.class);

  @Override
  protected String getDebugString(Embedding<K> embedding) {
    return String.format("([%s],[%s])",
      StringUtils.join(convertList(Arrays.asList(
        embedding.getVertexMapping()), true), ' '),
      StringUtils.join(convertList(Arrays.asList(
        embedding.getEdgeMapping()), false), ' '));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
