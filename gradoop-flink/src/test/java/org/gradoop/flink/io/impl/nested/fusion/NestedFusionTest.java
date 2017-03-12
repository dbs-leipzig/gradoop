package org.gradoop.flink.io.impl.nested.fusion;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.nested.datastructures.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.operators.nesting.Nesting;
import org.gradoop.flink.model.impl.nested.operators.union.Union;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.datastructures.NormalizedGraph;
import org.gradoop.flink.model.impl.nested.utils.RepresentationUtils;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class NestedFusionTest extends GradoopFlinkTestBase {

  /**
   * Defining the hashing functions required to break down the join function
   */
  protected FlinkAsciiGraphLoader getBibNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream("/data/gdl/jointest.gdl");
    return getLoaderFromStream(inputStream);
  }

  @Test
  public void nestWithNothing() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();
    GradoopId id = GradoopId.get();

    GraphCollection empty = loader.getGraphCollectionByVariables();
    DataLake driedLake = new DataLake(empty);
    IdGraphDatabase emptyIds = driedLake.getIdDatabase();

    GraphCollection data = loader.getGraphCollectionByVariables("research","citation");
    DataLake dataLake = new DataLake(data);
    IdGraphDatabase idbDataLake = dataLake.getIdDatabase();

    Nesting op = new Nesting(id);
    IdGraphDatabase result = dataLake.run(op).with(idbDataLake,emptyIds);

    Union union = new Union(id);
    IdGraphDatabase unionOp = dataLake.run(union).with(idbDataLake);

    // Checking if the graph returned is the same as the input one
    collectAndAssertTrue(RepresentationUtils.dataSetEquality(result.getGraphHeadToVertex(),
      unionOp.getGraphHeadToVertex()));
    collectAndAssertTrue(RepresentationUtils.dataSetEquality(result.getGraphHeadToEdge(),
      unionOp.getGraphHeadToEdge()));
    collectAndAssertTrue(result.asNormalizedGraph(dataLake)
      .equalsByData(unionOp.asNormalizedGraph(dataLake)));
  }

  @Test
  public void nestWithSingleGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();
    GradoopId id = GradoopId.get();

    // Loading the input with all the operands
    GraphCollection data = loader.getGraphCollectionByVariables("research","g0","g1","g2","g3",
      "g4");
    DataLake dataLake = new DataLake(data);

    // Loading just the part to be summarized
    DataLake operand = dataLake.extractGraphFromLabel("RG");
    IdGraphDatabase idbDataLake = operand.getIdDatabase();


    // Relations or nesting
    DataLake hgDataLake = dataLake.extractGraphFromLabel("first","second","third","fourth","fifth");
    IdGraphDatabase hypervertices = hgDataLake.getIdDatabase();

    // Loading the operator and evaluating the result through indices
    Nesting op = new Nesting(id);
    IdGraphDatabase result = dataLake.run(op).with(idbDataLake,hypervertices);
    NormalizedGraph normalizedResult = result.asNormalizedGraph(dataLake);

    // Loading the expected result from the dataset
    LogicalGraph expected = loader.getLogicalGraphByVariable("resulting");
    NormalizedGraph expectedGraph = new DataLake(expected).asNormalizedGraph();

    collectAndAssertTrue(expectedGraph.equalsByData(normalizedResult));
  }

  @Test
  public void nestWithGraphCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();
    GradoopId id = GradoopId.get();

    // Loading the input with all the operands
    GraphCollection data = loader.getGraphCollectionByVariables("research","citation","g0","g1","g2","g3",
      "g4");
    DataLake dataLake = new DataLake(data);

    // Loading just the part to be summarized
    DataLake operand = dataLake.extractGraphFromLabel("RG","CG");
    IdGraphDatabase idbDataLake = operand.getIdDatabase();

    // Relations or nesting
    DataLake hgDataLake = dataLake.extractGraphFromLabel("first","second","third","fourth","fifth");
    IdGraphDatabase hypervertices = hgDataLake.getIdDatabase();

    // Loading the operator and evaluating the result through indices
    Nesting op = new Nesting(id);
    IdGraphDatabase result = dataLake.run(op).with(idbDataLake,hypervertices);
    NormalizedGraph normalizedResult = result.asNormalizedGraph(dataLake);

    // Loading the expected result from the dataset
    LogicalGraph expected = loader.getLogicalGraphByVariable("result");
    NormalizedGraph expectedGraph = new DataLake(expected).asNormalizedGraph();

    collectAndAssertTrue(expectedGraph.equalsByData(normalizedResult));
  }

}
