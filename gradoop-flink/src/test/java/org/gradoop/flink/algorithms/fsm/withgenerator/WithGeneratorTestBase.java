package org.gradoop.flink.algorithms.fsm.withgenerator;

import org.gradoop.flink.algorithms.fsm.TransactionalFSMWithGeneratorTestBase;
import org.junit.runners.Parameterized;

import java.util.Arrays;


public abstract class WithGeneratorTestBase extends TransactionalFSMWithGeneratorTestBase {

  WithGeneratorTestBase(String testName, String directed, String threshold, String graphCount){
    super(testName, directed, threshold, graphCount);
  }

  @Parameterized.Parameters(name = "{index} : {0}")
  public static Iterable data(){
    return Arrays.asList(
      new String[] {
        "Directed_1.0_10",
        "true",
        "1.0",
        "10"
      },
      new String[] {
        "Directed_0.8_10",
        "true",
        "0.8",
        "10"
      },
      new String[] {
        "Directed_0.6_10",
        "true",
        "0.6",
        "10"
      },
      new String[] {
        "Undirected_1.0_10",
        "false",
        "1.0f",
        "10"
      },
      new String[] {
        "Undirected_0.8_10",
        "false",
        "0.8f",
        "10"
      },
      new String[] {
        "Undirected_0.6_10",
        "false",
        "0.6f",
        "10"
      }
    );
  }
}
