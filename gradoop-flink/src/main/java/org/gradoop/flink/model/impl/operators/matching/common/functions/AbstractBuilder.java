
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
