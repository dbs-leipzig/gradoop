package org.gradoop.model.operators;

import org.gradoop.model.Vertex;

/**
 * Created by martin on 02.12.14.
 */
public interface AttribtePredicate {
  boolean evaluate(Vertex v);
}
