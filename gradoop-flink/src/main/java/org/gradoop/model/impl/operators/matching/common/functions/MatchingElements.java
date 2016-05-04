package org.gradoop.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;

/**
 * Base class for matching EPGM elements.
 *
 * @param <EL> EPGM element type
 */
public abstract class MatchingElements<EL extends EPGMElement>
  extends RichFilterFunction<EL> {

  private final String query;

  private transient QueryHandler queryHandler;

  public MatchingElements(final String query) {
    this.query = query;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = QueryHandler.fromString(query);
  }

  protected QueryHandler getQueryHandler() {
    return queryHandler;
  }
}
