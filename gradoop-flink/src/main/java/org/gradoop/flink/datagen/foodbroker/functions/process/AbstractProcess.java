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

package org.gradoop.flink.datagen.foodbroker.functions.process;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Abstract class for the brokerage and the complaint handling process.
 */
public abstract class AbstractProcess extends AbstractRichFunction {
  /**
   * Foodbroker configuration.
   */
  protected FoodBrokerConfig config;
  /**
   * Iterator over all customers.
   */
  private Iterator<Map.Entry<GradoopId, Float>> customerIterator;
  /**
   * Iterator over all vendors.
   */
  private Iterator<Map.Entry<GradoopId, Float>> vendorIterator;
  /**
   * Iterator over all logistics.
   */
  private Iterator<Map.Entry<GradoopId, Float>> logisticIterator;
  /**
   * Iterator over all employees.
   */
  private Iterator<Map.Entry<GradoopId, Float>> employeeIterator;
  /**
   * Iterator over all product prices.
   */
  private Iterator<Map.Entry<GradoopId, Float>> productQualityIterator;
  /**
   * Iterator over all products.
   */
  protected Iterator<Map.Entry<GradoopId, BigDecimal>> productPriceIterator;
  /**
   * Graph ids, one seperate id for each case.
   */
  protected GradoopIdSet graphIds;
  /**
   * EPGM graph head factory.
   */
  protected GraphHeadFactory graphHeadFactory;
  /**
   * EPGM vertex factory.
   */
  protected VertexFactory vertexFactory;
  /**
   * EPGM edge factory.
   */
  protected EdgeFactory edgeFactory;
  /**
   * Map to quickly receive the target id of an edge.
   * Note that a object may have multiple outgoing edges with the same label.
   */
  protected Map<Tuple2<String, GradoopId>, Set<Edge>> edgeMap;
  /**
   * Map to get the vertex object of a given gradoop id.
   */
  protected Map<GradoopId, Vertex> vertexMap;
  /**
   * Map to get the customer quality of a given gradoop id.
   */
  private Map<GradoopId, Float> customerMap;
  /**
   * Map to get the vendor quality of a given gradoop id.
   */
  private Map<GradoopId, Float> vendorMap;
  /**
   * Map to get the logistic quality of a given gradoop id.
   */
  Map<GradoopId, Float> logisticMap;
  /**
   * Map to get the employee quality of a given gradoop id.
   */
  Map<GradoopId, Float> employeeMap;
  /**
   * Map to get the product quality of a given gradoop id.
   */
  Map<GradoopId, Float> productQualityMap;
  /**
   * Map to get the product price of a given gradoop id.
   */
  Map<GradoopId, BigDecimal> productPriceMap;
  /**
   * Map to get the user of a given gradoop id.
   */
  Map<GradoopId, Float> userMap;
  /**
   * The current id.
   */
  protected long currentId = 1;
  /**
   * A global seed.
   */
  protected long globalSeed;

  /**
   * Valued constructor.
   *
   * @param graphHeadFactory EPGM graph head factory
   * @param vertexFactory EPGM vertex factory
   * @param edgeFactory EPGM edge Factory
   * @param config FoodBroker configuration
   */
  public AbstractProcess(GraphHeadFactory graphHeadFactory, VertexFactory vertexFactory,
    EdgeFactory edgeFactory, FoodBrokerConfig config) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
    this.config = config;

    vertexMap = Maps.newHashMap();
    edgeMap = Maps.newHashMap();
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //get broadcasted maps
    customerMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.CUSTOMER_MAP).get(0);

    vendorMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.VENDOR_MAP).get(0);

    logisticMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.LOGISTIC_MAP).get(0);

    employeeMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.EMPLOYEE_MAP).get(0);

    productQualityMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.PRODUCT_QUALITY_MAP).get(0);
    //get the iterator of each map
    customerIterator = customerMap.entrySet().iterator();
    vendorIterator = vendorMap.entrySet().iterator();
    logisticIterator = logisticMap.entrySet().iterator();
    employeeIterator = employeeMap.entrySet().iterator();
    productQualityIterator = productQualityMap.entrySet().iterator();
  }

  /**
   * Creates a business identifier for transactional data.
   *
   * @param seed the transactional data seed
   * @param acronym the transactional data acronym
   * @return a busines identifier
   */
  protected String createBusinessIdentifier(long seed, String acronym) {
    String seedString = String.valueOf(seed);
    String idString = String.valueOf(globalSeed);
    int count = 6 - idString.length();
    for(int i = 1; i <= (count); i++) {
      idString = "0" + idString;
    }
    count = 6 - seedString.length();
    for(int i = 1; i <= (count); i++) {
      seedString = "0" + seedString;
    }
    return acronym + idString + "-" + seedString;
  }

  /**
   * Creates a new edge from the given fields.
   *
   * @param label the edge label
   * @param source the source id
   * @param target the target id
   */
  protected Edge newEdge(String label, GradoopId source, GradoopId target) {
    return newEdge(label, source, target, null);
  }

  /**
   * Creates a new edge from the given fields.
   *
   * @param label the edge label
   * @param source the source id
   * @param target the target id
   * @param properties the edge properties
   */
  protected Edge newEdge(String label, GradoopId source, GradoopId target,
    Properties properties) {
    Edge edge;
    if (properties == null) {
      edge = edgeFactory.createEdge(label, source, target, graphIds);
    } else {
      edge = edgeFactory.createEdge(label, source, target, properties, graphIds);
    }
    Tuple2<String, GradoopId> key = new Tuple2<>(label, source);
    Set<Edge> targets = Sets.newHashSet();
    if (edgeMap.containsKey(key)) {
      targets = edgeMap.get(key);
    }
    targets.add(edge);
    edgeMap.put(key, targets);
    return edge;
  }

  /**
   * Creates a new vertex and stores it in a map.
   *
   * @param label the new vertex label
   * @param properties the new vertex properties
   * @return the created vertex.
   */
  protected Vertex newVertex(String label, Properties properties) {
    Vertex vertex = vertexFactory.createVertex(label, properties, graphIds);
    vertexMap.put(vertex.getId(), vertex);
    return vertex;
  }

  /**
   * Searches the master data tuple which is the edge target of the given edge parameter.
   *
   * @param edgeLabel label of the edge
   * @param source source id
   * @return target master data tupel
   */
  protected GradoopId getEdgeTargetId(String edgeLabel, GradoopId source) {
    //there is always only one master data in this kind of edges
    return edgeMap.get(new Tuple2<>(edgeLabel, source)).iterator().next().getTargetId();
  }

  /**
   * Returns the quality of the target of the specified edge.
   *
   * @param edgeLabel label of the edge
   * @param source the source id of the edge
   * @param masterDataMap the map where the target id and the vertex are stored in
   * @return quality of the target of the edge
   */
  protected Float getEdgeTargetQuality(String edgeLabel, GradoopId source, String masterDataMap) {
    GradoopId target = getEdgeTargetId(edgeLabel, source);

    switch (masterDataMap) {
    case Constants.CUSTOMER_MAP:
      return customerMap.get(target);
    case Constants.VENDOR_MAP:
      return vendorMap.get(target);
    case Constants.LOGISTIC_MAP:
      return logisticMap.get(target);
    case Constants.EMPLOYEE_MAP:
      return employeeMap.get(target);
    case Constants.USER_MAP:
      return userMap.get(target);
    default:
      return null;
    }
  }

  /**
   * Returns the next customer id in the list.
   *
   * @return the next customer id
   */
  protected GradoopId getNextCustomer() {
    if (!customerIterator.hasNext()) {
      customerIterator = customerMap.entrySet().iterator();
    }
    return customerIterator.next().getKey();
  }

  /**
   * Returns the next vendor id in the list.
   *
   * @return the next vendor id
   */
  protected GradoopId getNextVendor() {
    if (!vendorIterator.hasNext()) {
      vendorIterator = vendorMap.entrySet().iterator();
    }
    return vendorIterator.next().getKey();
  }

  /**
   * Returns the next logistic id in the list.
   *
   * @return the next logistic id
   */
  protected GradoopId getNextLogistic() {
    if (!logisticIterator.hasNext()) {
      logisticIterator = logisticMap.entrySet().iterator();
    }
    return logisticIterator.next().getKey();
  }

  /**
   * Returns the next employee id in the list.
   *
   * @return the next employee id
   */
  protected GradoopId getNextEmployee() {
    if (!employeeIterator.hasNext()) {
      employeeIterator = employeeMap.entrySet().iterator();
    }
    return employeeIterator.next().getKey();
  }

  /**
   * Returns the next product id in the list.
   *
   * @return the next product id
   */
  protected GradoopId getNextProduct() {
    if (!productQualityIterator.hasNext()) {
      productQualityIterator = productQualityMap.entrySet().iterator();
      productPriceIterator = productPriceMap.entrySet().iterator();
    }
    //also apply to the price iterator
    productPriceIterator.next();
    return productQualityIterator.next().getKey();
  }

  /**
   * Returns a set of all created vertices.
   *
   * @return set of vertices
   */
  protected Set<Vertex> getVertices() {
    return Sets.newHashSet(vertexMap.values());
  }

  /**
   * Returns a set of all created edges.
   *
   * @return set of edges
   */
  protected Set<Edge> getEdges() {
    Set<Edge> edges = Sets.newHashSet();
    for(Map.Entry<Tuple2<String, GradoopId>, Set<Edge>> entry :
      edgeMap.entrySet()) {
      edges.addAll(entry.getValue());
    }
    return edges;
  }
}
