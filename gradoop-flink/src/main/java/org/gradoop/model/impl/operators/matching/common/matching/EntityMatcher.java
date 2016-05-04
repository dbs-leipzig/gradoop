package org.gradoop.model.impl.operators.matching.common.matching;

import org.gradoop.model.api.EPGMElement;
import org.s1ck.gdl.model.GraphElement;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Contains methods to match query entities and database entities.
 */
public class EntityMatcher {

  public static <EL1 extends EPGMElement, EL2 extends GraphElement>
  boolean match(EL1 dbElement, Collection<EL2> queryElements) {

    boolean match = false;

    for (GraphElement queryElement : queryElements) {
      match = match(dbElement, queryElement);
      // no need to verify remaining elements
      if (match) {
        break;
      }
    }
    return match;
  }

  public static <EL1 extends EPGMElement, EL2 extends GraphElement>
  boolean match(EL1 dbElement, EL2 queryElement) {

    boolean match  = false;
    // verify label
    if(queryElement.getLabel().equals(dbElement.getLabel())) {
      match = true;
    }
    // verify properties
    if (match && queryElement.getProperties() != null) {
      Iterator<Map.Entry<String, Object>> queryProperties =
        queryElement.getProperties().entrySet().iterator();

      while (match && queryProperties.hasNext()) {
        Map.Entry<String, Object> queryProperty = queryProperties.next();
        boolean validKey = dbElement.hasProperty(queryProperty.getKey());
        boolean validValue = dbElement.getPropertyValue(
          queryProperty.getKey()).getObject().equals(queryProperty.getValue());
        match = validKey && validValue;
      }
    }
    return match;
  }
}
