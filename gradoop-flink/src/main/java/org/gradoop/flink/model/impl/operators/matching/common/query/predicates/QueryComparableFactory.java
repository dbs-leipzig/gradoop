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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.s1ck.gdl.model.comparables.ComparableExpression;
import java.io.Serializable;

/**
 * Factory for @link{QueryComparable}s in order to support integration of newer GDL versions
 * in gradoop-temporal
 */
public abstract class QueryComparableFactory implements Serializable {

  /**
   * Creates a {@link QueryComparable} from a GDL comparable expression
   * @param comparable comparable to create wrapper for
   * @return QueryComparable
   */
  public abstract QueryComparable createFrom(ComparableExpression comparable);
}
