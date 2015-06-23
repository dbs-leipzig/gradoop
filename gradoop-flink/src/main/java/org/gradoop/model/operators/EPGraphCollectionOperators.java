/*
 * This file is part of Gradoop.
 *
 *     Gradoop is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Foobar is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.operators;

import org.gradoop.model.helper.Algorithm;
import org.gradoop.model.helper.BinaryFunction;
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;

/**
 * Created by martin on 18.06.15.
 */
public interface EPGraphCollectionOperators extends EPCollectionOperators {

  /*
  collection operators
   */

  EPGraphCollection select(Predicate<EPGraph> predicateFunction);

  EPGraphCollection union(EPGraphCollection otherCollection);

  EPGraphCollection intersect(EPGraphCollection otherCollection);

  EPGraphCollection difference(EPGraphCollection otherCollection);

  EPGraphCollection distinct();

  EPGraphCollection sortBy(String propertyKey, Order order);

  EPGraphCollection top(int limit);

  /*
  auxiliary operators
   */

  EPGraphCollection apply(UnaryFunction<EPGraph, EPGraph> unaryFunction);

  EPGraph reduce(BinaryFunction<EPGraph, EPGraph> binaryGraphOperator);

  EPGraph callForGraph(Algorithm algorithm, String... params);

  EPGraphCollection callForCollection(Algorithm algorithm, String... params);
}
