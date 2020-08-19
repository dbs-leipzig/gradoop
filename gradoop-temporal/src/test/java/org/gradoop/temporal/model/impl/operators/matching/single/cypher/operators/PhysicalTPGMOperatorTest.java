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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class PhysicalTPGMOperatorTest extends GradoopFlinkTestBase {

  protected DataSet<TemporalVertex> createVerticesWithProperties(List<String> propertyNames) {
    Properties properties = getProperties(propertyNames);
    TemporalVertexFactory vertexFactory = new TemporalVertexFactory();

    List<TemporalVertex> vertices = Lists.newArrayList(
      vertexFactory.createVertex("Label1", properties),
      vertexFactory.createVertex("Label2", properties)
    );

    return getExecutionEnvironment().fromCollection(vertices);
  }

  protected DataSet<TemporalEdge> createEdgesWithProperties(List<String> propertyNames) {
    Properties properties = getProperties(propertyNames);
    TemporalEdgeFactory edgeFactory = new TemporalEdgeFactory();

    List<TemporalEdge> edges = Lists.newArrayList(
      edgeFactory.createEdge("Label1", GradoopId.get(), GradoopId.get(), properties),
      edgeFactory.createEdge("Label2", GradoopId.get(), GradoopId.get(), properties)
    );

    return getExecutionEnvironment().fromCollection(edges);
  }

  protected PropertyValue[] getPropertyValues(List<String> propertyNames) {
    PropertyValue[] propertyValues = new PropertyValue[propertyNames.size()];

    int i = 0;
    for (String propertyName : propertyNames) {
      propertyValues[i++] = PropertyValue.create(propertyName);
    }

    return propertyValues;
  }

  protected Properties getProperties(List<String> propertyNames) {
    Properties properties = new Properties();

    for (String propertyName : propertyNames) {
      properties.set(propertyName, propertyName);
    }

    return properties;
  }

  protected CNF predicateFromQuery(String query) throws QueryContradictoryException {
    // no default asOf-predicates:
    /*if (!query.contains("WHERE")) {
      query += " WHERE tx_to.after(1970-01-01T00:00:00)";
    } else {
      query += " AND tx_to.after(1970-01-01T00:00:00)";
    }*/
    return new TemporalQueryHandler(query, new CNFPostProcessing(new ArrayList<>()))
      .getPredicates();
  }
}

