/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.algorithms;

import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * Base class for all BIIIG related unit tests. Contains a few sample graphs
 * which can be used in specific tests.
 */
public abstract class GiraphTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private String getCallingMethod() {
    return Thread.currentThread().getStackTrace()[1].getMethodName();
  }

  protected Path getTempFile(String fileName)
      throws IOException {
    fileName =
        (fileName != null) ? fileName : getCallingMethod() + "_" + new Random()
            .nextLong();
    File tmpFile = testFolder.newFile(fileName);
    return new Path(tmpFile.getPath());
  }

  /**
   * @return a small graph with two BTGs that are connected
   */
  protected String[] getConnectedIIG() {
    return new String[] {
        "0,1 0,1 4 9 10",
        "1,1 1,0 5 6 11 12",
        "2,1 2,8 13",
        "3,1 3,7 14 15",
        "4,0 4,0 5",
        "5,0 5,1 4 6",
        "6,0 6,1 5 7 8",
        "7,0 7,3 6",
        "8,0 8,2 6",
        "9,0 9,0 10",
        "10,0 10,0 9 11 12",
        "11,0 11,1 10 13 14",
        "12,0 12,1 10 15",
        "13,0 13,2 11",
        "14,0 14,3 11",
        "15,0 15,3 12"
    };
  }

  /**
   * @return a small graph with two BTGs that are disconnected
   */
  protected String[] getDisconnectedIIG() {
    return new String[] {
        "0,1 0,6 7",
        "1,1 1,2 7",
        "2,1 2,1 8 9",
        "3,1 3,4 10",
        "4,1 4,3 5 11 12",
        "5,1 5,4 12 13",
        "6,0 6,0 7",
        "7,0 7,0 1 6 8",
        "8,0 8,2 7 9",
        "9,0 9,2 8",
        "10,0 10,3 11 12",
        "11,0 11,4 10",
        "12,0 12,4 5 10 13",
        "13,0 13,5"
    };
  }

  /**
   * @return a small graph with two BTGs that are connected, each vertex has
   * correct BTG ids assigned
   */
  protected String[] getConnectedIIGWithBTGIDs() {
    return new String[] {
        "0,1 0 0 1,1 4 9 10",
        "1,1 1 0 1,0 5 6 11 12",
        "2,1 2 0 1,8 13",
        "3,1 3 0 1,7 14 15",
        "4,0 4 0,0 5",
        "5,0 5 0,1 4 6",
        "6,0 6 0,1 5 7 8",
        "7,0 7 0,3 6",
        "8,0 8 0,2 6",
        "9,0 9 1,0 10",
        "10,0 10 1,0 9 11 12",
        "11,0 11 1,1 10 13 14",
        "12,0 12 1,1 10 15",
        "13,0 13 1,2 11",
        "14,0 14 1,3 11",
        "15,0 15 1,3 12"
    };
  }

  /**
   * @return a small graph with two BTGs that are disconnected, each vertex has
   * correct BTG ids assigned
   */
  protected String[] getDisconnectedIIGWithBTGIDs() {
    return new String[] {
        "0,1 0,6 7",
        "1,1 1,2 7",
        "2,1 2,1 8 9",
        "3,1 3,4 10",
        "4,1 4,3 5 11 12",
        "5,1 5,4 12 13",
        "6,0 6,0 7",
        "7,0 7,0 1 6 8",
        "8,0 8,2 7 9",
        "9,0 9,2 8",
        "10,0 10,3 11 12",
        "11,0 11,4 10",
        "12,0 12,4 5 10 13",
        "13,0 13,5"
    };
  }

  // a graph containing a single vertex of type Master
  protected String[] getSingleMasterVertexIIG() {
    return new String[] {
        "0,1 0"
    };
  }

  protected String[] getTwoMasterVerticesIIG() {
    return new String[] {
        "0,1 0",
        "1,1 0,0"
    };
  }

  // a graph containing a single vertex of type Transactional
  protected String[] getSingleTransactionalVertexIIG() {
    return new String[] {
        "0,0 0"
    };
  }

  // a graph containing a single vertex of type Master and an associated BTG
  protected String[] getSingleMasterVertexIIGWithBTG() {
    return new String[] {
        "0,1 0.0 0"
    };
  }

  // a graph containing a single vertex of type Transactional and an
  // associated BTG
  protected String[] getSingleTransactionalVertexIIGWithBTG() {
    return new String[] {
        "0,0 0.0 0"
    };
  }
}
