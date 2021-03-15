/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.query;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Util {

  public static void equalCNFs(CNF a, CNF b) {
    assertEquals(a.size(), b.size());
    for (CNFElement c1 : a.getPredicates()) {
      List<ComparisonExpression> c1Comps = c1.getPredicates();
      c1Comps.sort(java.util.Comparator.comparingInt(ComparisonExpression::hashCode));
      boolean found = false;
      for (CNFElement c2 : b.getPredicates()) {
        List<ComparisonExpression> c2Comps = c2.getPredicates();
        c2Comps.sort(java.util.Comparator.comparingInt(ComparisonExpression::hashCode));
        if (c2Comps.equals(c1Comps)) {
          found = true;
          break;
        }
      }
      if (!found) {
        fail();
      }
    }
  }

  public static boolean comparisonEqual(ComparisonExpression a, ComparisonExpression b) {
    QueryComparable lhsA = a.getLhs();
    QueryComparable rhsA = a.getRhs();
    QueryComparable lhsB = b.getLhs();
    QueryComparable rhsB = b.getRhs();
    Comparator compA = a.getComparator();
    Comparator compB = b.getComparator();

    if (lhsA.equals(lhsB) && (compA.equals(compB)) && rhsA.equals(rhsB)) {
      return true;
    }

    Comparator compASwitched = a.switchSides().getComparator();
    if (lhsA.equals(rhsB) && (compASwitched.equals(compB)) && rhsA.equals(lhsB)) {
      return true;
    }
    return false;
  }

}
