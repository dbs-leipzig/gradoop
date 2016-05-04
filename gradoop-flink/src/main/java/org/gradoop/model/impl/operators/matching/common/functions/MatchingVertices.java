package org.gradoop.model.impl.operators.matching.common.functions;

import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.matching.common.matching.EntityMatcher;

/**
 * Filter vertices based on their occurrence in the given GDL pattern.
 *
 * @param <V> EPGM vertex type
 */
public class MatchingVertices<V extends EPGMVertex>
  extends MatchingElements<V> {

  /**
   * Create new filter.
   *
   * @param query GDL query string
   */
  public MatchingVertices(final String query) {
    super(query);
  }

  @Override
  public boolean filter(V v) throws Exception {
    boolean match = EntityMatcher.match(v, getQueryHandler().getVertices());
//    System.out.println(String.format("(%d:%s) => %s",
//      v.getPropertyValue("id").getInt(),
//      v.getLabel(),
//      match));
    return match;
  }
}
