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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;

/**
 * Base class for filtering EPGM elements based on their matches.
 *
 * @param <EL>  EPGM element type
 */
public abstract class AbstractFilter<EL extends Element> extends RichFilterFunction<EL> {

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
  public AbstractFilter(final String query) {
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
}
