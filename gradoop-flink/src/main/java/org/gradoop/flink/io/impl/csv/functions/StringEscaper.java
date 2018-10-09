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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Escapes characters in strings and allows to split escaped strings.
 */
public class StringEscaper {
  /**
   * Escape character.
   */
  private static final char ESCAPE_CHARACTER = '\\';
  /**
   * Custom escape sequences to avoid disruptive behavior of the file reader (e.g. newline).
   */
  private static final BiMap<Character, CharSequence> CUSTOM_ESCAPE_SEQUENCES =
    new ImmutableBiMap.Builder<Character, CharSequence>()
      .put('\t', String.format("%c%c", ESCAPE_CHARACTER, 't'))
      .put('\b', String.format("%c%c", ESCAPE_CHARACTER, 'b'))
      .put('\n', String.format("%c%c", ESCAPE_CHARACTER, 'n'))
      .put('\r', String.format("%c%c", ESCAPE_CHARACTER, 'r'))
      .put('\f', String.format("%c%c", ESCAPE_CHARACTER, 'f'))
      .build();

  /**
   * Escapes the {@code escapedCharacters} in a string.
   *
   * @param string string to be escaped
   * @param escapedCharacters characters to be escaped
   * @return escaped string
   */
  public static String escape(String string, Set<Character> escapedCharacters) {
    StringBuilder sb = new StringBuilder();
    for (char c : string.toCharArray()) {
      if (escapedCharacters.contains(c)) {
        sb.append(escapeCharacter(c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /**
   * Unescapes the escaped characters in a string.
   *
   * @param escapedString string to be unescaped
   * @return unescaped string
   */
  public static String unescape(String escapedString) {
    StringBuilder sb = new StringBuilder();
    boolean escaped = false;
    for (int i = 0; i < escapedString.length(); i++) {
      if (escaped) {
        escaped = false;
        sb.append(unescapeSequence(escapedString.subSequence(i - 1, i + 1)));
      } else if (escapedString.charAt(i) == ESCAPE_CHARACTER) {
        escaped = true;
      } else {
        sb.append(escapedString.charAt(i));
      }
    }
    return sb.toString();
  }

  /**
   * Splits an escaped string while ignoring escaped delimiters. Does not unescape the tokens.
   *
   * @param escapedString escaped string to be split
   * @param delimiter delimiter string
   * @return string array with still escaped strings split by the delimiter
   * @throws IllegalArgumentException if the delimiter contains the escape character
   */
  public static String[] split(String escapedString, String delimiter)
    throws IllegalArgumentException {
    return split(escapedString, delimiter, 0);
  }

  /**
   * Splits an escaped string while ignoring escaped delimiters. Does not unescape the tokens.
   *
   * @param escapedString escaped string to be split
   * @param delimiter delimiter string
   * @param limit limits the size of the output
   * @return string array with still escaped strings split by the delimiter
   * @throws IllegalArgumentException if the delimiter contains the escape character
   */
  public static String[] split(String escapedString, String delimiter, int limit)
    throws IllegalArgumentException {
    if (delimiter.contains(Character.toString(ESCAPE_CHARACTER))) {
      throw new IllegalArgumentException(String.format(
        "Delimiter must not contain the escape character: '%c'", ESCAPE_CHARACTER));
    }
    if (limit <= 0) {
      limit = escapedString.length() + 1;
    }

    List<String> tokens = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    boolean escaped = false;
    int delimiterIndex = 0;
    for (char c : escapedString.toCharArray()) {
      // parse and match delimiter
      if (!escaped && c == delimiter.charAt(delimiterIndex)) {
        delimiterIndex++;
        if (delimiterIndex == delimiter.length()) {
          if (tokens.size() < limit - 1) {
            tokens.add(sb.toString());
            sb.setLength(0);
          } else {
            sb.append(delimiter, 0, delimiterIndex);
          }
          delimiterIndex = 0;
        }
      } else {
        // reset delimiter parsing
        sb.append(delimiter, 0, delimiterIndex);
        delimiterIndex = 0;

        // escape
        if (escaped) {
          escaped = false;
        } else if (c == ESCAPE_CHARACTER) {
          escaped = true;
        }

        sb.append(c);
      }
    }
    sb.append(delimiter, 0, delimiterIndex);
    tokens.add(sb.toString());
    return tokens.toArray(new String[0]);
  }

  /**
   * Returns the escape sequence of a given character.
   *
   * @param character character to be escaped
   * @return escape sequence
   */
  private static CharSequence escapeCharacter(char character) {
    if (CUSTOM_ESCAPE_SEQUENCES.containsKey(character)) {
      return CUSTOM_ESCAPE_SEQUENCES.get(character);
    }
    return String.format("%c%c", ESCAPE_CHARACTER, character);
  }

  /**
   * Returns the character of a given escape sequence.
   *
   * @param sequence escape sequence
   * @return escaped character
   */
  private static char unescapeSequence(CharSequence sequence) {
    if (CUSTOM_ESCAPE_SEQUENCES.containsValue(sequence)) {
      return CUSTOM_ESCAPE_SEQUENCES.inverse().get(sequence);
    }
    return sequence.charAt(1);
  }
}
