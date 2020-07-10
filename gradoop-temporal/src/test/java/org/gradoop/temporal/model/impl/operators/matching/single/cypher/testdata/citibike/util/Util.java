/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.util;

import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Utility methods for citibike data tests.
 */
public class Util {

  /**
   * Loads data from a file in res/patternmatchingtest/complex
   *
   * @param path file path
   * @return test case data to be used in
   * {@link org.gradoop.temporal.model.impl.operators.matching.ASCIITemporalPatternMatchingTest}
   */
  public static ArrayList<String[]> getDataFromFile(String path) {
    ArrayList<String[]> data = new ArrayList<>();

    Scanner scanner = null;
    try {
      scanner = new Scanner(new File(path));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    while (scanner.hasNextLine()) {
      String id = scanner.nextLine();
      String pattern = scanner.nextLine();
      String where = scanner.nextLine();
      String results = scanner.nextLine();
      data.add(buildTestCase(id, pattern, where, results));
    }

    return data;
  }

  /**
   * Creates a test case string array as in the other tests from data given in the file
   *
   * @param id      id of the testcase read from the file
   * @param pattern graph pattern of the testcase read from the file
   * @param where   where conditions of the testcase read from the file
   * @param results results of the testcase read from the file
   * @return testcase string array as used in the other {@code HomomorphismXYZData.java}
   */
  private static String[] buildTestCase(String id, String pattern, String where, String results) {
    String[] testCase = new String[5];

    testCase[0] = id;
    testCase[1] = CBCypherTemporalPatternMatchingTest.defaultData;

    String query = "MATCH " + pattern + " WHERE " + where;
    testCase[2] = CBCypherTemporalPatternMatchingTest.noDefaultAsOf(query);

    String[] res = results.split(",");
    if (res[0].equals("---")) {
      testCase[3] = "";
      testCase[4] = "";
      return testCase;
    }

    StringBuilder exp = new StringBuilder();
    StringBuilder resultBuilder = new StringBuilder();
    for (int i = 0; i < res.length; i++) {
      exp.append("expected").append(i + 1).append(",");
      resultBuilder.append("expected").append(i + 1).append("[")
        .append(res[i].trim()).append("], ");
    }
    testCase[3] = new String(exp).substring(0, exp.length() - 1);
    testCase[4] = new String(resultBuilder).substring(0, resultBuilder.length() - 2);

    return testCase;
  }

  @Test
  public void test() {
    ArrayList<String[]> data = getDataFromFile(
      "src/test/resources/data/patternmatchingtest/complex/complex_queries_homomorphism");
    System.out.println(data.size());
    for (int i = 0; i < data.size(); i++) {
      System.out.println(data.get(i)[0]);
      System.out.println(data.get(i)[1]);
      System.out.println(data.get(i)[2]);
      System.out.println(data.get(i)[3]);
      System.out.println(data.get(i)[4]);
      System.out.println();
    }
  }
}
