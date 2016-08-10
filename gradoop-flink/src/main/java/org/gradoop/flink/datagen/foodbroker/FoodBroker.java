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
package org.gradoop.flink.datagen.foodbroker;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage.FoodBrokerage;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage
  .MasterDataMapFromTuple;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage
  .MasterDataQualityMapper;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage.ProductPriceMapper;
import org.gradoop.flink.datagen.foodbroker.masterdata.CustomerGenerator;
import org.gradoop.flink.datagen.foodbroker.masterdata.EmployeeGenerator;
import org.gradoop.flink.datagen.foodbroker.masterdata.LogisticsGenerator;
import org.gradoop.flink.datagen.foodbroker.masterdata.ProductGenerator;
import org.gradoop.flink.datagen.foodbroker.masterdata.VendorGenerator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.model.api.operators.CollectionGenerator;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Generates a GraphCollection containing a foodbrokerage and a complaint
 * handling process.
 */
public class FoodBroker implements CollectionGenerator {
  /**
   * Execution environment
   */
  protected final ExecutionEnvironment env;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig gradoopFlinkConfig;
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
    GradoopFlinkConfig gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {

    this.env = env;
    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.foodBrokerConfig = foodBrokerConfig;
  }

  @Override
  public GraphCollection execute() {

    // used for type hinting when loading graph head data
    TypeInformation graphHeadTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getGraphHeadFactory().getType());
    // used for type hinting when loading vertex data
    TypeInformation vertexTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation edgeTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getEdgeFactory().getType());

    DataSet customers =
      new CustomerGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet vendors =
      new VendorGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet logistics =
      new LogisticsGenerator(gradoopFlinkConfig, foodBrokerConfig)
        .generate();

    DataSet employees =
      new EmployeeGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet products =
      new ProductGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Long> caseSeeds = env.generateSequence(1, foodBrokerConfig
      .getCaseCount());

    FoodBrokerage foodBrokerage = new FoodBrokerage(
      gradoopFlinkConfig.getGraphHeadFactory(),
      gradoopFlinkConfig.getVertexFactory(),
      gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig);

    DataSet<Map<GradoopId, Float>> customerDataMap = customers
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> vendorDataMap = vendors
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> logisticDataMap = logistics
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> employeeDataMap = employees
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> productQualityDataMap = products
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, BigDecimal>> productPriceDataMap = products
      .map(new ProductPriceMapper())
      .reduceGroup(new MasterDataMapFromTuple<BigDecimal>());



    DataSet<GraphTransaction> foodBrokerageTransactions = caseSeeds
      .mapPartition(foodBrokerage)
      .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
      .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
      .withBroadcastSet(logisticDataMap, Constants.LOGISTIC_MAP)
      .withBroadcastSet(employeeDataMap, Constants.EMPLOYEE_MAP)
      .withBroadcastSet(productQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
      .withBroadcastSet(productPriceDataMap, Constants.PRODUCT_PRICE_MAP)
      .returns(GraphTransaction.getTypeInformation(gradoopFlinkConfig));



    DataSet transactionalVertices = foodBrokerageTransactions
      .map(new GraphTransactionTriple())
      .flatMap(new TransactionVertices())
      .returns(vertexTypeInfo);

    DataSet transactionalEdges = foodBrokerageTransactions
      .map(new GraphTransactionTriple())
      .flatMap(new TransactionEdges())
      .returns(edgeTypeInfo);

    DataSet graphHeads = foodBrokerageTransactions
      .map(new GraphTransactionTriple())
      .map(new TransactionGraphHead())
      .returns(graphHeadTypeInfo);

    DataSet masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .join(transactionalEdges)
      .where(new Id()).equalTo(new TargetId())

      .with(new JoinFunction<Vertex, Edge, Vertex>() {
        @Override
        public Vertex join(Vertex v, Edge e) throws Exception {
          v.setGraphIds(e.getGraphIds());
          return v;
        }
      });

 DataSet vertices = masterData
   .union(transactionalVertices);

    return GraphCollection.fromDataSets(graphHeads, vertices,
      transactionalEdges, gradoopFlinkConfig);
  }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }
}
