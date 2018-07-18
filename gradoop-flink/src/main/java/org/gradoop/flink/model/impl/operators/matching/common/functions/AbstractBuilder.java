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
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;

import java.util.List;

/**
 * Base class for mapping EPGM elements to representations for mapping.
 *
 * @param <EL> EPGM element type
 * @param <OUT> output type
 */
public abstract class AbstractBuilder<EL extends Element, OUT> extends RichMapFunction<EL, OUT> {

  /**
   * GDL query
   */
  private final String query;

  /**
   * Query handler
   */
  private transient QueryHandler queryHandler;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public AbstractBuilder(final String query) {
    this.query = query;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = new QueryHandler(query);
  }

  protected QueryHandler getQueryHandler() {
    return queryHandler;
  }

  /**
   * Returns a bit vector representing the matches for the given entity.
   *
   * @param candidateCount  size of the bit vector
   * @param matches         matches for the given entity
   * @return bit vector representing matches
   */
  protected boolean[] getCandidates(int candidateCount, List<Long> matches) {
    boolean[] candidates = new boolean[candidateCount];

    for (Long candidate : matches) {
      candidates[candidate.intValue()] = true;
    }
    return candidates;
  }
}
