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
package org.gradoop.flink.model.impl.operators.matching.common.matching;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.pojo.Element;
import org.s1ck.gdl.model.GraphElement;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Contains methods to match query and data graph elements.
 */
public class ElementMatcher {

  /**
   * Matches the given data graph element against all given query elements.
   *
   * @param <EL1>         EPGM element type
   * @param <EL2>         GDL element type
   * @param dbElement     data graph element (vertex/edge)
   * @param queryElements query graph elements (vertices/edges)
   * @param defaultLabel  default element label
   * @return true, iff the data graph element matches at least one query element
   */
  public static <EL1 extends Element, EL2 extends GraphElement>
  boolean matchAll(EL1 dbElement, Collection<EL2> queryElements, String defaultLabel) {

    boolean match = false;

    for (GraphElement queryElement : queryElements) {
      match = match(dbElement, queryElement, defaultLabel);
      // no need to verify remaining elements
      if (match) {
        break;
      }
    }
    return match;
  }

  /**
   * Returns all query candidate ids for the given EPGM element.
   *
   * @param dbElement     EPGM element (vertices/edges)
   * @param queryElements query graph elements (vertices/edges)
   * @param <EL1>         EPGM element type
   * @param <EL2>         GDL element type
   * @param defaultLabel  default element label
   * @return all candidate ids for {@code dbElement}
   */
  public static <EL1 extends Element, EL2 extends GraphElement>
  List<Long> getMatches(EL1 dbElement, Collection<EL2> queryElements, String defaultLabel) {

    List<Long> matches = Lists.newArrayListWithCapacity(queryElements.size());

    for (GraphElement queryElement : queryElements) {
      if (match(dbElement, queryElement, defaultLabel)) {
        matches.add(queryElement.getId());
      }
    }

    return matches;
  }

  /**
   * Matches the given data graph element against the given query element.
   *
   * @param <EL1>         EPGM element type
   * @param <EL2>         GDL element type
   * @param dbElement     data graph element (vertex/edge)
   * @param queryElement  query graph element (vertex/edge)
   * @param defaultLabel  default element label
   * @return true, iff the data graph element matches the query graph element
   */
  public static <EL1 extends Element, EL2 extends GraphElement>
  boolean match(EL1 dbElement, EL2 queryElement, String defaultLabel) {

    boolean match  = false;
    // verify label
    if (queryElement.getLabel().equals(dbElement.getLabel()) ||
      queryElement.getLabel().equals(defaultLabel)) {
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
