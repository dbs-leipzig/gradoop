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
package org.gradoop.flink.io.impl.csv.functions;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests the string escaper
 */
public class StringEscaperTest {
  /**
   * Characters to be escaped
   */
  private static final Set<Character> ESCAPED_CHARACTERS = new HashSet<>(
    Arrays.asList(';', ',', '|', ':', '\n'));
  /**
   * CSV string to be escaped
   */
  private static final String UNESCAPED_STRING = "tes\t\\,f\nsfg,:,d|";
  /**
   * Escaped CSV string
   */
  private static final String ESCAPED_STRING = "tes\t\\\\\\,f\\nsfg\\,\\:\\,d\\|";

  /**
   * Test escaping a string
   */
  @Test
  public void testEscape() {
    String escapedString = StringEscaper.escape(UNESCAPED_STRING, ESCAPED_CHARACTERS);
    assertEquals(ESCAPED_STRING, escapedString);
  }

  /**
   * Test unescaping a string
   */
  @Test
  public void testUnescape() {
    String string = StringEscaper.unescape(ESCAPED_STRING);
    assertEquals(UNESCAPED_STRING, string);
  }

  /**
   * Test splitting an escaped string
   */
  @Test
  public void testSplit() {
    List<String> strings = Arrays.asList("abc;;", "ad,", "|\n\n df", "\\");
    String[] input = strings.stream()
      .map(s -> StringEscaper.escape(s, ESCAPED_CHARACTERS))
      .toArray(String[]::new);

    String delimiter = ",";
    assertArrayEquals(input, StringEscaper.split(String.join(delimiter, input), delimiter));

    delimiter = "c;";
    assertArrayEquals(input, StringEscaper.split(String.join(delimiter, input), delimiter));

    delimiter = "\n\n.;|";
    assertArrayEquals(input, StringEscaper.split(String.join(delimiter, input), delimiter));
  }

  /**
   * Test splitting an escaped string with the escape character in the delimiter
   */
  @Test(expected = IllegalArgumentException.class)
  public void testSplitWithIllegalDelimiter() {
    StringEscaper.split("String", "abc\\def");
  }
}
