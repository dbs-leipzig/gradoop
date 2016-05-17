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
