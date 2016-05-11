package org.gradoop.model.impl.datagen.foodbroker.generators;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Collection;

public class CaseGenerator{

  private final FoodBrokerConfig foodBrokerConfig;
  private final ExecutionEnvironment env;

  public CaseGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    this.foodBrokerConfig = foodBrokerConfig;
    this.env = gradoopFlinkConfig.getExecutionEnvironment();  }

  public DataSet<GradoopId> generate() {

    Integer caseCount = foodBrokerConfig.getCaseCount();

    if(caseCount == 0) {
      caseCount = 10;
    }

    Collection<GradoopId> caseSeeds = Lists
      .newArrayListWithCapacity(caseCount);

    for(int i = 1; i <= caseCount; i++) {
      caseSeeds.add(GradoopId.get());
    }

    return env
      .fromCollection(caseSeeds);
  }
}
