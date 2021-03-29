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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators;

import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;

import java.util.ArrayList;
import java.util.List;

public abstract class BasePhysicalTPGMOperatorTest extends GradoopFlinkTestBase {

  protected Properties getProperties(List<String> propertyNames) {
    Properties properties = new Properties();

    for (String propertyName : propertyNames) {
      properties.set(propertyName, propertyName);
    }

    return properties;
  }

  protected CNF predicateFromQuery(String query) throws QueryContradictoryException {
    return new TemporalQueryHandler(query, new CNFPostProcessing(new ArrayList<>()))
      .getPredicates();
  }
}

