package org.gradoop.model.impl.operators.matching.common.functions;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.operators.matching.common.matching.EntityMatcher;

/**
 * Filter edges based on their occurrence in the given GDL pattern.
 *
 * @param <E> EPGM edge type
 */
public class MatchingEdges<E extends EPGMEdge>
  extends MatchingElements<E> {

  public MatchingEdges(final String query) {
    super(query);
  }

  @Override
  public boolean filter(E e) throws Exception {
    boolean match = EntityMatcher.match(e, getQueryHandler().getEdges());
//    System.out.println(String.format("[%d:%s] => %s",
//      e.getPropertyValue("id").getInt(),
//      e.getLabel(),
//      match));
    return match;
  }
}
