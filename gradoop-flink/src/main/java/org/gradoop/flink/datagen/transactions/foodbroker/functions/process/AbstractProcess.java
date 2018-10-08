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
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerBroadcastNames;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;

import java.math.BigDecimal;
import java.util.List;
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
  protected Map<GradoopId, Vertex> logisticIndex;
  /**
   * Map to get the employee quality of a given gradoop id.
   */
  protected Map<GradoopId, Vertex> employeeIndex;
  /**
   * Map to get the product quality of a given gradoop id.
   */
  protected Map<GradoopId, Vertex> productIndex;

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
  protected Map<GradoopId, Vertex> customerIndex;
  /**
   * List of all employees.
   */
  protected GradoopId[] employeeList;
  /**
   * Map to get the vendor quality of a given gradoop id.
   */
  private Map<GradoopId, Vertex> vendorIndex;
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
   * List of all product prices.
   */
  private GradoopId[] productList;

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
    customerIndex = createIndexFromBroadcast(FoodBrokerBroadcastNames.BC_CUSTOMERS);
    vendorIndex = createIndexFromBroadcast(FoodBrokerBroadcastNames.BC_VENDORS);
    logisticIndex = createIndexFromBroadcast(FoodBrokerBroadcastNames.BC_LOGISTICS);
    employeeIndex = createIndexFromBroadcast(FoodBrokerBroadcastNames.BC_EMPLOYEES);

    productIndex = createIndexFromBroadcast(FoodBrokerBroadcastNames.BC_PRODUCTS);

    //get the iterator of each map
    customerList = customerIndex.keySet().toArray(new GradoopId[customerIndex.keySet().size()]);
    vendorList = vendorIndex.keySet().toArray(new GradoopId[vendorIndex.keySet().size()]);
    logisticList = logisticIndex.keySet().toArray(new GradoopId[logisticIndex.keySet().size()]);
    employeeList = employeeIndex.keySet().toArray(new GradoopId[employeeIndex.keySet().size()]);
    productList = productIndex.keySet()
      .toArray(new GradoopId[productIndex.keySet().size()]);
  }

  /**
   * DRY-prevention to create vertex indices from broadcast variables.
   *
   * @param broadcastVariable broadcast variable
   * @return vertex index
   */
  private Map<GradoopId, Vertex> createIndexFromBroadcast(String broadcastVariable) {

    List<Vertex> broadcast =
      getRuntimeContext().getBroadcastVariable(broadcastVariable);

    Map<GradoopId, Vertex> map = Maps.newHashMapWithExpectedSize(broadcast.size());

    for (Vertex vertex : broadcast) {
      map.put(vertex.getId(), vertex);
    }

    return map;
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
    case FoodBrokerBroadcastNames.BC_CUSTOMERS:
      return getQuality(customerIndex, target);
    case FoodBrokerBroadcastNames.BC_VENDORS:
      return getQuality(vendorIndex, target);
    case FoodBrokerBroadcastNames.BC_LOGISTICS:
      return getQuality(logisticIndex, target);
    case FoodBrokerBroadcastNames.BC_EMPLOYEES:
      return getQuality(employeeIndex, target);
    case FoodBrokerBroadcastNames.USER_MAP:
      return userMap.get(target);
    default:
      return null;
    }
  }

  /**
   * Convenience method to get a vertex's quality value.
   *
   * @param index vertex index
   * @param id vertex id
   * @return quality
   */
  protected float getQuality(Map<GradoopId, Vertex> index, GradoopId id) {
    return index.get(id).getPropertyValue(FoodBrokerPropertyKeys.QUALITY_KEY).getFloat();
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
    float influence = 1.0f;
    String firstCity = "1";
    String firstHolding = "1";
    String secondCity = "2";
    String secondHolding = "2";

    switch (firstMasterDataMap) {
    case FoodBrokerBroadcastNames.BC_CUSTOMERS:
      firstCity = getStringValue(customerIndex, firstMasterDataId, FoodBrokerPropertyKeys.CITY_KEY);
      firstHolding =
        getStringValue(customerIndex, firstMasterDataId, FoodBrokerPropertyKeys.HOLDING_KEY);
      break;
    case FoodBrokerBroadcastNames.BC_VENDORS:
      firstCity = getStringValue(vendorIndex, firstMasterDataId, FoodBrokerPropertyKeys.CITY_KEY);
      firstHolding =
        getStringValue(vendorIndex, firstMasterDataId, FoodBrokerPropertyKeys.HOLDING_KEY);
      break;
    case FoodBrokerBroadcastNames.BC_EMPLOYEES:
      firstCity = employeeIndex
        .get(firstMasterDataId).getPropertyValue(FoodBrokerPropertyKeys.CITY_KEY).getString();
      break;
    default:
      break;
    }

    switch (secondMasterDataMap) {
    case FoodBrokerBroadcastNames.BC_CUSTOMERS:
      secondCity =
        getStringValue(customerIndex, secondMasterDataId, FoodBrokerPropertyKeys.CITY_KEY);
      secondHolding =
        getStringValue(customerIndex, secondMasterDataId, FoodBrokerPropertyKeys.HOLDING_KEY);
      break;
    case FoodBrokerBroadcastNames.BC_VENDORS:
      secondCity =
        getStringValue(vendorIndex, secondMasterDataId, FoodBrokerPropertyKeys.CITY_KEY);
      secondHolding =
        getStringValue(vendorIndex, secondMasterDataId, FoodBrokerPropertyKeys.HOLDING_KEY);
      break;
    case FoodBrokerBroadcastNames.BC_EMPLOYEES:
      secondCity = employeeIndex
        .get(firstMasterDataId).getPropertyValue(FoodBrokerPropertyKeys.CITY_KEY).getString();
      break;
    default:
      break;
    }

    if (firstCity.equals(secondCity)) {
      influence *= config.getMasterDataSameCityInfluence();
    }
    if (firstHolding.equals(secondHolding) &&
      !firstHolding.equals(FoodBrokerPropertyKeys.HOLDING_TYPE_PRIVATE)) {

      influence *= config.getMasterDataSameHoldingInfluence();
    }
    return influence;
  }

  /**
   * Convenience method to read a vertex's string property value.
   *
   * @param index vertex index
   * @param id vertex id
   * @param key property key
   * @return string value
   */
  protected String getStringValue(Map<GradoopId, Vertex> index, GradoopId id, String key) {
    return index.get(id).getPropertyValue(key).getString();
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
    return getNextMasterData(this.customerList, this.customerIndex);
  }

  /**
   * Returns the next random vendor id in the list.
   *
   * @return the next random vendor id
   */
  protected GradoopId getNextVendor() {
    return getNextMasterData(this.vendorList, this.vendorIndex);
  }

  /**
   * Returns the next random logistic id in the list.
   *
   * @return the next random logistic id
   */
  protected GradoopId getNextLogistic() {
    return getNextMasterData(this.logisticList, this.logisticIndex);
  }

  /**
   * Returns the next random employee id in the list.
   *
   * @return the next random employee id
   */
  protected GradoopId getNextEmployee() {
    return getNextMasterData(this.employeeList, this.employeeIndex);
  }

  /**
   * Get a random master data id.
   *
   * @param list id list
   * @param index master data index
   * @return master data id
   */
  private GradoopId getNextMasterData(GradoopId[] list, Map<GradoopId, Vertex> index) {
    GradoopId id = getRandomEntryFromArray(list);
    vertexMap.put(id, index.get(id));
    return id;
  }

  /**
   * Returns the next random product id in the list.
   *
   * @return the next random product id
   */
  protected GradoopId getNextProduct() {
    return getNextMasterData(this.productList, this.productIndex);
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

  /**
   * Returns the price of a product with a given id.
   *
   * @param productId product
   * @return price
   */
  protected BigDecimal getPrice(GradoopId productId) {
    return productIndex
      .get(productId).getPropertyValue(FoodBrokerPropertyKeys.PRICE_KEY).getBigDecimal();
  }
}
