/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;

import java.util.ArrayList;

/**
 * Removes duplicate clauses from the CNF
 */
public class Deduplication implements QueryTransformation {

  @Override
  public CNF transformCNF(CNF cnf) throws QueryContradictoryException {
    ArrayList<CNFElement> newClauses = new ArrayList<>();
    for (CNFElement clause : cnf.getPredicates()) {
      if (!newClauses.contains(clause)) {
        newClauses.add(clause);
      }
    }
    return new CNF(newClauses);
  }
}
