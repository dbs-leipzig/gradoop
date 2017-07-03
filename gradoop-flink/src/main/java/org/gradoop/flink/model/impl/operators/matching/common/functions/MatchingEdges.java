
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.matching.ElementMatcher;

import java.util.Collection;

import static org.gradoop.common.util.GConstants.DEFAULT_EDGE_LABEL;

/**
 * Filter edges based on their occurrence in the given GDL pattern.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ReadFields("label;properties")
public class MatchingEdges<E extends Edge> extends AbstractFilter<E> {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;
  /**
   * Query edges to match against.
   */
  private transient Collection<org.s1ck.gdl.model.Edge> queryEdges;
  /**
   * Constructor
   *
   * @param query GDL query
   */
  public MatchingEdges(final String query) {
    super(query);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryEdges = getQueryHandler().getEdges();
  }

  @Override
  public boolean filter(E e) throws Exception {
    return ElementMatcher.matchAll(e, queryEdges, DEFAULT_EDGE_LABEL);
  }
}
