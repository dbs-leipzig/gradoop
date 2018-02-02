/**
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
package org.gradoop.flink.datagen.transactions.foodbroker.functions.process;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.BusinessRelationData;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.EmployeeData;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Abstract class for the brokerage and the complaint handling process.
 */
public abstract class AbstractProcess extends AbstractRichFunction {
  /**
   * EPGM graph head factory.
   */
  protected EPGMGraphHeadFactory<GraphHead> graphHeadFactory;
  /**
   * EPGM vertex factory.
   */
  protected EPGMVertexFactory<Vertex> vertexFactory;
  /**
   * EPGM edge factory.
   */
  protected EPGMEdgeFactory<Edge> edgeFactory;
  /**
   * Foodbroker configuration.
   */
  protected FoodBrokerConfig config;
  /**
   * Map to get the logistic quality of a given gradoop id.
   */
  protected Map<GradoopId, Float> logisticMap;
  /**
   * Map to get the employee quality of a given gradoop id.
   */
  protected Map<GradoopId, EmployeeData> employeeMap;
  /**
   * Map to get the product quality of a given gradoop id.
   */
  protected Map<GradoopId, Float> productQualityMap;
  /**
   * Map to get the product price of a given gradoop id.
   */
  protected Map<GradoopId, BigDecimal> productPriceMap;
  /**
   * Iterator over all products.
   */
  protected Iterator<Map.Entry<GradoopId, BigDecimal>> productPriceIterator;
  /**
   * Graph ids, one seperate id for each case.
   */
  protected GradoopIdSet graphIds;
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
   * Map to get the user of a given gradoop id.
   */
  protected Map<GradoopId, Float> userMap;
  /**
   * The current id.
   */
  protected long currentId = 1;
  /**
   * A global seed.
   */
  protected long globalSeed;
  /**
   * Map to get the customer quality of a given gradoop id.
   */
  private Map<GradoopId, BusinessRelationData> customerMap;
  /**
   * Map to get the vendor quality of a given gradoop id.
   */
  private Map<GradoopId, BusinessRelationData> vendorMap;
  /**
   * List of all customers.
   */
  private GradoopId[] customerList;
  /**
   * List of all vendors.
   */
  private GradoopId[] vendorList;
  /**
   * List of all logistics.
   */
  private GradoopId[] logisticList;
  /**
   * List of all employees.
   */
  private GradoopId[] employeeList;
  /**
   * List of all product prices.
   */
  private GradoopId[] productQualityList;

  /**
   * Valued constructor.
   *
   * @param graphHeadFactory EPGM graph head factory
   * @param vertexFactory EPGM vertex factory
   * @param edgeFactory EPGM edge Factory
   * @param config FoodBroker configuration
   */
  public AbstractProcess(EPGMGraphHeadFactory<GraphHead> graphHeadFactory,
    EPGMVertexFactory<Vertex> vertexFactory,
    EPGMEdgeFactory<Edge> edgeFactory, FoodBrokerConfig config) {
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
    customerMap = getRuntimeContext().<Map<GradoopId, BusinessRelationData>>
      getBroadcastVariable(Constants.CUSTOMER_MAP_BC).get(0);

    vendorMap = getRuntimeContext().<Map<GradoopId, BusinessRelationData>>
      getBroadcastVariable(Constants.VENDOR_MAP_BC).get(0);

    logisticMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.LOGISTIC_MAP_BC).get(0);

    employeeMap = getRuntimeContext().<Map<GradoopId, EmployeeData>>
      getBroadcastVariable(Constants.EMPLOYEE_MAP_BC).get(0);

    productQualityMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.PRODUCT_QUALITY_MAP_BC).get(0);
    //get the iterator of each map
    customerList = customerMap.keySet().toArray(new GradoopId[customerMap.keySet().size()]);
    vendorList = vendorMap.keySet().toArray(new GradoopId[vendorMap.keySet().size()]);
    logisticList = logisticMap.keySet().toArray(new GradoopId[logisticMap.keySet().size()]);
    employeeList = employeeMap.keySet().toArray(new GradoopId[employeeMap.keySet().size()]);
    productQualityList = productQualityMap.keySet()
      .toArray(new GradoopId[productQualityMap.keySet().size()]);
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
    for (int i = 1; i <= count; i++) {
      idString = "0" + idString;
    }
    count = 6 - seedString.length();
    for (int i = 1; i <= count; i++) {
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
   * @return the newly created edge
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
   * @return the newly created edge
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
    return getEdgeTargetQuality(target, masterDataMap);
  }

  /**
   * Returns the quality of the target of the specified edge.
   *
   * @param target gradoop id of the target
   * @param masterDataMap the map where the target id and the vertex are stored in
   * @return quality of the target of the edge
   */
  protected Float getEdgeTargetQuality(GradoopId target, String masterDataMap) {
    switch (masterDataMap) {
    case Constants.CUSTOMER_MAP_BC:
      return customerMap.get(target).getQuality();
    case Constants.VENDOR_MAP_BC:
      return vendorMap.get(target).getQuality();
    case Constants.LOGISTIC_MAP_BC:
      return logisticMap.get(target);
    case Constants.EMPLOYEE_MAP_BC:
      return employeeMap.get(target).getQuality();
    case Constants.USER_MAP:
      return userMap.get(target);
    default:
      return null;
    }
  }

  /**
   * Calculates additional relative influence for two master data objects. The addition is
   * increased if the objects share the same location or the same holding. If not 1.0f is returned.
   *
   * @param firstMasterDataId gradoop id of the first master data object
   * @param firstMasterDataMap name of the map of the first master data object
   * @param secondMasterDataId gradoop id of the second master data object
   * @param secondMasterDataMap name of the map of the second master data object
   * @return float value representing relative addition to the master data objects qualities
   */
  protected Float getAdditionalInfluence(
    GradoopId firstMasterDataId, String firstMasterDataMap,
    GradoopId secondMasterDataId, String secondMasterDataMap) {
    Float influence = 1.0f;
    String firstCity = "1";
    String firstHolding = "1";
    String secondCity = "2";
    String secondHolding = "2";

    switch (firstMasterDataMap) {
    case Constants.CUSTOMER_MAP_BC:
      firstCity = customerMap.get(firstMasterDataId).getCity();
      firstHolding = customerMap.get(firstMasterDataId).getHolding();
      break;
    case Constants.VENDOR_MAP_BC:
      firstCity = vendorMap.get(firstMasterDataId).getCity();
      firstHolding = vendorMap.get(firstMasterDataId).getHolding();
      break;
    case Constants.EMPLOYEE_MAP_BC:
      firstCity = employeeMap.get(firstMasterDataId).getCity();
      break;
    default:
      break;
    }

    switch (secondMasterDataMap) {
    case Constants.CUSTOMER_MAP_BC:
      secondCity = customerMap.get(secondMasterDataId).getCity();
      secondHolding = customerMap.get(secondMasterDataId).getHolding();
      break;
    case Constants.VENDOR_MAP_BC:
      secondCity = vendorMap.get(secondMasterDataId).getCity();
      secondHolding = vendorMap.get(secondMasterDataId).getHolding();
      break;
    case Constants.EMPLOYEE_MAP_BC:
      secondCity = employeeMap.get(secondMasterDataId).getCity();
      break;
    default:
      break;
    }

    if (firstCity.equals(secondCity)) {
      influence *= config.getMasterDataSameCityInfluence();
    }
    if (firstHolding.equals(secondHolding) &&
      !firstHolding.equals(Constants.HOLDING_TYPE_PRIVATE)) {

      influence *= config.getMasterDataSameHoldingInfluence();
    }
    return influence;
  }

  /**
   * Returns a random id in the array.
   *
   * @param array array containing gradoop ids
   * @return a random entry
   */
  protected GradoopId getRandomEntryFromArray(GradoopId[] array) {
    return array[new Random().nextInt(array.length)];
  }

  /**
   * Returns the next random customer id in the list.
   *
   * @return the next random customer id
   */
  protected GradoopId getNextCustomer() {
    return getRandomEntryFromArray(customerList);
  }

  /**
   * Returns the next random vendor id in the list.
   *
   * @return the next random vendor id
   */
  protected GradoopId getNextVendor() {
    return getRandomEntryFromArray(vendorList);
  }

  /**
   * Returns the next random logistic id in the list.
   *
   * @return the next random logistic id
   */
  protected GradoopId getNextLogistic() {
    return getRandomEntryFromArray(logisticList);
  }

  /**
   * Returns the next random employee id in the list.
   *
   * @return the next random employee id
   */
  protected GradoopId getNextEmployee() {
    return getRandomEntryFromArray(employeeList);
  }

  /**
   * Returns the next random product id in the list.
   *
   * @return the next random product id
   */
  protected GradoopId getNextProduct() {
    return getRandomEntryFromArray(productQualityList);
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
    for (Map.Entry<Tuple2<String, GradoopId>, Set<Edge>> entry : edgeMap.entrySet()) {
      edges.addAll(entry.getValue());
    }
    return edges;
  }
}
