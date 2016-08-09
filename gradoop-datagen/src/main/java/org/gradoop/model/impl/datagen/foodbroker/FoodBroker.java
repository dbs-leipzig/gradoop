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
package org.gradoop.model.impl.datagen.foodbroker;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.CollectionGenerator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.*;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.MasterDataQualityMapper;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.CustomerGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.EmployeeGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.LogisticsGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.ProductGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.VendorGenerator;
import org.gradoop.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.TargetId;
import org.gradoop.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Generates a GraphCollection containing a foodbrokerage and a complaint
 * handling process.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class FoodBroker
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements CollectionGenerator<G, V, E> {
  /**
   * Execution environment
   */
  protected final ExecutionEnvironment env;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig<G, V, E> gradoopFlinkConfig;
  /**
   * Foodbroker configuration
   */
  private final FoodBrokerConfig foodBrokerConfig;

  /**
   * Valued constructor.
   *
   * @param env execution environment
   * @param gradoopFlinkConfig Gradoop Flink configuration
   * @param foodBrokerConfig Foodbroker configuration
   */
  public FoodBroker(ExecutionEnvironment env,
    GradoopFlinkConfig<G, V, E> gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {

    this.env = env;
    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.foodBrokerConfig = foodBrokerConfig;
  }

  @Override
  public GraphCollection<G, V, E> execute() {

    // used for type hinting when loading graph head data
    TypeInformation<G> graphHeadTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getGraphHeadFactory().getType());
    // used for type hinting when loading vertex data
    TypeInformation<V> vertexTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<E> edgeTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getEdgeFactory().getType());

    DataSet<V> customers =
      new CustomerGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> vendors =
      new VendorGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> logistics =
      new LogisticsGenerator<V>(gradoopFlinkConfig, foodBrokerConfig)
        .generate();

    DataSet<V> employees =
      new EmployeeGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> products =
      new ProductGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Long> caseSeeds = env.generateSequence(1, foodBrokerConfig
      .getCaseCount());

    FoodBrokerage<G, V, E> foodBrokerage = new FoodBrokerage<G, V, E>(
      gradoopFlinkConfig.getGraphHeadFactory(),
      gradoopFlinkConfig.getVertexFactory(),
      gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig);

    DataSet<Map<GradoopId, Float>> customerDataMap = customers
      .map(new MasterDataQualityMapper<V>())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> vendorDataMap = vendors
      .map(new MasterDataQualityMapper<V>())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> logisticDataMap = logistics
      .map(new MasterDataQualityMapper<V>())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> employeeDataMap = employees
      .map(new MasterDataQualityMapper<V>())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> productQualityDataMap = products
      .map(new MasterDataQualityMapper<V>())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, BigDecimal>> productPriceDataMap = products
      .map(new ProductPriceMapper<V>())
      .reduceGroup(new MasterDataMapFromTuple<BigDecimal>());



    DataSet<GraphTransaction<G, V, E>> foodBrokerageTransactions = caseSeeds
      .mapPartition(foodBrokerage)
      .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
      .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
      .withBroadcastSet(logisticDataMap, Constants.LOGISTIC_MAP)
      .withBroadcastSet(employeeDataMap, Constants.EMPLOYEE_MAP)
      .withBroadcastSet(productQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
      .withBroadcastSet(productPriceDataMap, Constants.PRODUCT_PRICE_MAP)
      .returns(GraphTransaction.getTypeInformation(gradoopFlinkConfig));



    DataSet<V> transactionalVertices = foodBrokerageTransactions
      .map(new GraphTransactionTriple<G, V, E>())
      .flatMap(new TransactionVertices<G, V, E>())
      .returns(vertexTypeInfo);

    DataSet<E> transactionalEdges = foodBrokerageTransactions
      .map(new GraphTransactionTriple<G, V, E>())
      .flatMap(new TransactionEdges<G, V, E>())
      .returns(edgeTypeInfo);

    DataSet<G> graphHeads = foodBrokerageTransactions
      .map(new GraphTransactionTriple<G, V, E>())
      .map(new TransactionGraphHead<G, V, E>())
      .returns(graphHeadTypeInfo);

    DataSet<V> masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .join(transactionalEdges)
      .where(new Id<V>()).equalTo(new TargetId<E>())
      .with(new JoinFunction<V, E, V>() {
        @Override
        public V join(V v, E e) throws Exception {
          v.setGraphIds(e.getGraphIds());
          return v;
        }
      });

 DataSet<V> vertices = masterData
   .union(transactionalVertices);

    return GraphCollection.fromDataSets(graphHeads, vertices,
      transactionalEdges, gradoopFlinkConfig);
  }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }
}
