/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
public abstract class AbstractBuilder<EL extends Element, OUT>
  extends RichMapFunction<EL, OUT> {

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
