package org.gradoop.flink.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage.MasterDataMapFromTuple;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage.MasterDataQualityMapper;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage.ProductPriceMapper;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Created by Stephan on 18.08.16.
 */
public abstract class AbstractBusinessProcess implements BusinessProcess {

  /**
   * Execution environment
   */
  private ExecutionEnvironment env;
  /**
   * Gradoop Flink configuration
   */
  protected GradoopFlinkConfig gradoopFlinkConfig;
  /**
   * Foodbroker configuration
   */
  protected FoodBrokerConfig foodBrokerConfig;

  protected DataSet<GraphTransaction> graphTransactions;

  protected DataSet<Vertex> customers;

  private DataSet<Vertex> vendors;

  private DataSet<Vertex> logistics;

  protected DataSet<Vertex> employees;

  private DataSet<Vertex> products;

  protected DataSet<Long> caseSeeds;

  protected DataSet<Map<GradoopId, Float>> customerDataMap;
  protected DataSet<Map<GradoopId, Float>> vendorDataMap;
  protected DataSet<Map<GradoopId, Float>> logisticDataMap;
  protected DataSet<Map<GradoopId, Float>> employeeDataMap;
  protected DataSet<Map<GradoopId, Float>> productQualityDataMap;
  protected DataSet<Map<GradoopId, BigDecimal>> productPriceDataMap;

  public AbstractBusinessProcess() {
  }

  public AbstractBusinessProcess(FoodBrokerConfig foodBrokerConfig,
    GradoopFlinkConfig gradoopFlinkConfig, ExecutionEnvironment env,
    DataSet<Vertex> customers, DataSet<Vertex> vendors,
    DataSet<Vertex> logistics, DataSet<Vertex> employees,
    DataSet<Vertex> products, DataSet<Long> caseSeeds) {
    this.foodBrokerConfig = foodBrokerConfig;
    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.env = env;
    this.customers = customers;
    this.vendors = vendors;
    this.logistics = logistics;
    this.employees = employees;
    this.products = products;
    this.caseSeeds = caseSeeds;

    initMaps();
  }

  private void initMaps() {
    customerDataMap = customers
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    vendorDataMap = vendors
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    logisticDataMap = logistics
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    employeeDataMap = employees
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    productQualityDataMap = products
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    productPriceDataMap = products
      .map(new ProductPriceMapper())
      .reduceGroup(new MasterDataMapFromTuple<BigDecimal>());
  }

  @Override
  public DataSet<GraphTransaction> getTransactions() {
    return graphTransactions;
  }
}
