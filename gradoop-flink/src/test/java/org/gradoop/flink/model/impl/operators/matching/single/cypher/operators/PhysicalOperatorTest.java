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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;

import java.util.List;

import static org.junit.Assert.assertTrue;

public abstract class PhysicalOperatorTest extends GradoopFlinkTestBase {

  protected DataSet<Vertex> createVerticesWithProperties(List<String> propertyNames) {
    Properties properties = getProperties(propertyNames);
    VertexFactory vertexFactory = new VertexFactory();

    List<Vertex> vertices = Lists.newArrayList(
      vertexFactory.createVertex("Label1",properties),
      vertexFactory.createVertex("Label2",properties)
    );

    return getExecutionEnvironment().fromCollection(vertices);
  }

  protected DataSet<Edge> createEdgesWithProperties(List<String> propertyNames) {
    Properties properties = getProperties(propertyNames);
    EdgeFactory edgeFactory = new EdgeFactory();

    List<Edge> edges = Lists.newArrayList(
      edgeFactory.createEdge("Label1", GradoopId.get(), GradoopId.get(), properties),
      edgeFactory.createEdge("Label2", GradoopId.get(), GradoopId.get(), properties)
    );

    return getExecutionEnvironment().fromCollection(edges);
  }

  protected PropertyValue[] getPropertyValues(List<String> propertyNames) {
    PropertyValue[] propertyValues = new PropertyValue[propertyNames.size()];

    int i = 0;
    for(String property_name : propertyNames) {
      propertyValues[i++] = PropertyValue.create(property_name);
    }

    return propertyValues;
  }

  protected Properties getProperties(List<String> propertyNames) {
    Properties properties = new Properties();

    for(String property_name : propertyNames) {
      properties.set(property_name, property_name);
    }

    return properties;
  }

  protected CNF predicateFromQuery(String query) {
    return new QueryHandler(query).getPredicates();
  }
}
