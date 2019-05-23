/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.examples.frequentpattern.data;

/**
 * Provides example data for the DIMSpanExample
 */
public class DIMSpanData {

  /**
   * Private Constructor
   */
  private DIMSpanData() { }

  /**
   * Provides an example graph collection used for DIMSpanExample
   *
   * @return example graph collection
   */
  public static String getGraphGDLString() {

    return
      "g1[" +
      "   (v1:A)-->(v2:B)" +
      "   (v1)-->(v3:C)" +
      "]" +
      "g2[" +
      "   (v4:A)-->(v5:B)" +
      "   (v4)-->(v6:C)" +
      "   (v4)-->(v7:D)" +
      "   (v5)-->(v6)" +
      "]" +
      "g3[" +
      "   (v8:A)-->(v9:B)" +
      "   (v8)-->(v10:C)" +
      "   (v8)-->(v11:D)" +
      "   (v8)-->(v12:E)" +
      "   (v9)-->(v10)" +
      "]";
  }
}
