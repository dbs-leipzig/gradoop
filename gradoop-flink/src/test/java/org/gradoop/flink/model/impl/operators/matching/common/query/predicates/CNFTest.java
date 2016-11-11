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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .expressions.ComparisonWrapper;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.ElementSelector;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CNFTest {
  @Test
  public void andConjunctionTest() {
    CNFElement or1 = new CNFElement();
    CNFElement or2 = new CNFElement();

    CNF and1 = new CNF();
    and1.addPredicate(or1);

    CNF and2 = new CNF();
    and2.addPredicate(or2);

    ArrayList<CNFElement> reference = new ArrayList<>();
    reference.add(or1);
    reference.add(or2);

    assertEquals(reference, and1.and(and2).getPredicates());
  }

  @Test
  public void orConjunctionTest() {
    ComparisonWrapper a =
      new ComparisonWrapper(new Comparison(new Literal(1), Comparator.GT, new Literal(5)));
    ComparisonWrapper b =
      new ComparisonWrapper(new Comparison(new Literal(2), Comparator.GT, new Literal(6)));
    ComparisonWrapper c =
      new ComparisonWrapper(new Comparison(new Literal(3), Comparator.GT, new Literal(7)));
    ComparisonWrapper d =
      new ComparisonWrapper(new Comparison(new Literal(4), Comparator.GT, new Literal(8)));

    CNFElement or1 = new CNFElement();
    or1.addPredicate(a);
    CNFElement or2 = new CNFElement();
    or2.addPredicate(b);
    CNFElement or3 = new CNFElement();
    or3.addPredicate(c);
    CNFElement or4 = new CNFElement();
    or4.addPredicate(d);

    // Define a CNF of the Form [ [a], [b] ]
    CNF and1 = new CNF();
    and1.addPredicate(or1);
    and1.addPredicate(or2);

    // Define a CNF of the Form [ [c], [d] ]
    CNF and2 = new CNF();
    and2.addPredicate(or3);
    and2.addPredicate(or4);

    CNFElement refOr1 = new CNFElement();
    refOr1.addPredicate(a);
    refOr1.addPredicate(c);
    CNFElement refOr2 = new CNFElement();
    refOr2.addPredicate(a);
    refOr2.addPredicate(d);
    CNFElement refOr3 = new CNFElement();
    refOr3.addPredicate(b);
    refOr3.addPredicate(c);
    CNFElement refOr4 = new CNFElement();
    refOr4.addPredicate(b);
    refOr4.addPredicate(d);

    // Expected output is [ [a,c], [a,d], [b,c]. [b,d] ]
    CNF reference = new CNF();
    reference.addPredicate(refOr1);
    reference.addPredicate(refOr2);
    reference.addPredicate(refOr3);
    reference.addPredicate(refOr4);

    assertEquals(reference.toString(), and1.or(and2).toString());
  }

  @Test
  public void extractVariablesTest() {
    Comparison a = new Comparison(
            new ElementSelector("a"),
            Comparator.EQ,
            new ElementSelector("b")
    );

    Comparison b = new Comparison(
            new PropertySelector("a","label"),
            Comparator.EQ,
            new Literal("Person")
    );

    List<CNFElement> cnfElements = new ArrayList<>();
    CNFElement e1 = new CNFElement();
    e1.addPredicate(new ComparisonWrapper(a));
    cnfElements.add(e1);

    CNFElement e2 = new CNFElement();
    e1.addPredicate(new ComparisonWrapper(b));
    cnfElements.add(e2);

    CNF cnf = new CNF();
    cnf.addPredicates(cnfElements);

    Set<String> reference = new HashSet<>();
    reference.add("a");
    reference.add("b");

    assertEquals(reference,cnf.getVariables());
  }

  @Test
  public void createExistingSubCnfTest() {
    Comparison a = new Comparison(
            new ElementSelector("a"),
            Comparator.EQ,
            new ElementSelector("b")
    );

    Comparison b = new Comparison(
            new PropertySelector("a","label"),
            Comparator.EQ,
            new Literal("Person")
    );

    Comparison c = new Comparison(
            new PropertySelector("c","label"),
            Comparator.EQ,
            new Literal("Person")
    );

    List<CNFElement> cnfElements = new ArrayList<>();
    List<CNFElement> refCnfElements = new ArrayList<>();

    CNFElement e1 = new CNFElement();
    e1.addPredicate(new ComparisonWrapper(a));
    cnfElements.add(e1);
    refCnfElements.add(e1);

    CNFElement e2 = new CNFElement();
    e2.addPredicate(new ComparisonWrapper(b));
    cnfElements.add(e2);

    CNFElement e3 = new CNFElement();
    e3.addPredicate(new ComparisonWrapper(c));
    cnfElements.add(e3);

    CNF cnf = new CNF();
    cnf.addPredicates(cnfElements);

    CNF reference = new CNF();
    reference.addPredicates(refCnfElements);

    Set<String> variables = new HashSet<>();
    variables.add("a");
    variables.add("b");

    assertEquals(reference,cnf.getSubCNF(variables));

    variables.add("c");
    assertTrue(cnf.getSubCNF(variables).getPredicates().isEmpty());
  }
}
