/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.protocol.deser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class PartialJsonDeserializer {

  /**
   * Given a StringIterator over a serialized JSON object, advance the iterator through the object.
   * Any time we find an object key matching one of the consumers, we position the iterator at the
   * start of the value for that key and call the consumer with the iterator.
   * <p>
   * The consumers MUST fully read the value (including any nested objects), and MUST NOT read
   * anything after the value.
   * <p>
   * We intentionally define the consumers as accepting an iterator instead of the substring to avoid
   * duplicating the data in-memory when possible. Consumers may need to extract the substring, but
   * this method is designed to operate solely on the original copy of the string, even in recursive
   * calls.
   *
   * @return true if the object was non-null, false if it was null
   */
  public static boolean processObject(final StringIterator data, final Map<String, Consumer<StringIterator>> keyValueConsumers) {
    final char firstChar = data.peek();
    if (firstChar == 'n') {
      skipExactString(data, "null");
      return false;
    }

    skipWhitespaceAndCharacter(data, '{');
    skipWhitespace(data);
    // handle empty object specially
    if (data.peek() == '}') {
      data.next();
      return true;
    }
    while (data.hasNext()) {
      // Read a key/value pair
      final String key = readStringValue(data);
      skipWhitespaceAndCharacter(data, ':');
      skipWhitespace(data);
      // Pass to the appropriate consumer, or read past the value
      if (keyValueConsumers.containsKey(key)) {
        keyValueConsumers.get(key).accept(data);
      } else {
        skipValue(data);
      }

      // Check if we have another entry in the object
      skipWhitespace(data);
      final char ch = data.next();
      if (ch == '}') {
        return true;
      } else if (ch != ',') {
        throw new RuntimeException("Unexpected '" + ch + "'" + " at index " + data.getIndex());
      }
    }
    throw new RuntimeException("Unexpected end of string");
  }

  /**
   * Read a JSON value from the iterator and return it as a serialized string.
   */
  public static String readSerializedValue(final StringIterator data) {
    skipWhitespace(data);
    final int start = data.getIndex();
    skipValue(data);
    final int end = data.getIndex();
    // TODO maybe faster if we fill a stringbuilder while we're reading the value, rather than skipping
    // the value?
    return data.substring(start, end);
  }

  public static String readStringValue(final StringIterator data) {
    // TODO this is heavily copied from skipValue's string-handling branch, can we unify them?
    skipWhitespaceAndCharacter(data, '"');
    final StringBuilder sb = new StringBuilder();
    while (data.hasNext()) {
      final char ch = data.next();
      switch (ch) {
        case '"' -> {
          return sb.toString();
        }
        case '\\' -> {
          final char escapeChar = data.next();
          switch (escapeChar) {
            // Basic escape characters
            case '"' -> sb.append('"');
            case '\\' -> sb.append('\\');
            case '/' -> sb.append('/');
            case 'b' -> sb.append('\b');
            case 'f' -> sb.append('\f');
            case 'n' -> sb.append('\n');
            case 'r' -> sb.append('\r');
            case 't' -> sb.append('\t');
            // Unicode escape (e.g. "\uf00d")
            case 'u' -> {
              String hexString = "";
              for (int i = 0; i < 4; i++) {
                hexString += data.next();
              }
              // TODO is this correct?
              final int value = Integer.parseInt(hexString, 16);
              sb.append((char) value);
            }
            // Invalid escape
            default -> throw new RuntimeException("Invalid escape character '" + escapeChar + "'" + " at index " + data.getIndex());
          }
        }
        default -> sb.append(ch);
      }
    }
    throw new RuntimeException("Unexpected end of string");
  }

  public static Number readNumber(final StringIterator data) {
    final char firstChar = data.peek();
    if (firstChar == 'n') {
      skipExactString(data, "null");
      return null;
    }

    final int startIndex = data.getIndex();
    skipNumber(data);
    final String numberStr = data.substring(startIndex, data.getIndex());
    if (numberStr.contains(".")) {
      return Double.parseDouble(numberStr);
    } else {
      return Long.parseLong(numberStr);
    }
  }

  public static <T> List<T> readList(final StringIterator data, final Function<StringIterator, T> valueMapper) {
    // Skip the opening bracket and start reading the object
    final char firstChar = data.next();
    if (firstChar == 'n') {
      skipExactString(data, "ull");
      return null;
    }
    if (firstChar != '[') {
      throw new IllegalStateException("Unexpected '" + firstChar + "'" + " at index " + data.getIndex());
    }

    final List<T> list = new ArrayList<>();

    skipWhitespace(data);
    // Check for empty array
    if (data.peek() == ']') {
      data.next();
      return list;
    }
    // Loop over the array
    while (data.hasNext()) {
      list.add(valueMapper.apply(data));
      skipWhitespace(data);
      final char ch = data.next();
      if (ch == ']') {
        return list;
      } else if (ch != ',') {
        throw new RuntimeException("Unexpected '" + ch + "'" + " at index " + data.getIndex());
      }
    }

    throw new IllegalStateException("Unexpected end of input while processing list");
  }

  private static void skipValue(final StringIterator data) {
    skipWhitespace(data);
    final char firstChar = data.peek();
    switch (firstChar) {
      case '"' -> {
        // Skip the opening quote and start reading the string
        data.next();
        while (data.hasNext()) {
          final char ch = data.next();
          if (ch == '"') {
            return;
          } else if (ch == '\\') {
            final char escapeChar = data.next();
            switch (escapeChar) {
              // Basic escape characters
              case '"', '\\', '/', 'b', 'f', 'n', 'r', 't' -> {
                // do nothing, this is just part of the string literal
              }
              // Unicode escape (e.g. "\uf00d")
              case 'u' -> {
                for (int i = 0; i < 4; i++) {
                  final char expectedHexDigit = data.next();
                  final boolean isDigit = '0' <= expectedHexDigit && expectedHexDigit <= '9';
                  final boolean isLowercaseHexDigit = 'a' <= expectedHexDigit && expectedHexDigit <= 'f';
                  final boolean isUppercaseHexDigit = 'A' <= expectedHexDigit && expectedHexDigit <= 'F';
                  if (!isDigit && !isLowercaseHexDigit && !isUppercaseHexDigit) {
                    throw new RuntimeException("Expected hex digit but got '" + expectedHexDigit + "'" + " at index " + data.getIndex());
                  }
                }
              }
              // Invalid escape
              default -> throw new RuntimeException("Invalid escape character '" + escapeChar + "'" + " at index " + data.getIndex());
            }
          }
        }
        throw new RuntimeException("Unexpected end of string");
      }
      case '{' -> {
        // Skip the opening curly brace and start reading the object
        data.next();
        skipWhitespace(data);
        // handle empty object
        if (data.peek() == '}') {
          data.next();
          return;
        }
        // Otherwise, read a key/value pair
        if (data.peek() != '"') {
          // Keys must be strings.
          throw new RuntimeException("Expected '\"' at index " + data.getIndex() + " but got '" + data.peek() + "'");
        }
        skipValue(data);
        skipWhitespaceAndCharacter(data, ':');
        skipWhitespace(data);
        skipValue(data);
        // and then read the rest of the object
        readToEndOfObject(data);
      }
      case '[' -> {
        // Skip the opening bracket and start reading the object
        data.next();
        skipWhitespace(data);
        // Check for empty array
        if (data.peek() == ']') {
          data.next();
          return;
        }
        // Loop over the array
        while (data.hasNext()) {
          skipValue(data);
          skipWhitespace(data);
          final char ch = data.next();
          if (ch == ']') {
            return;
          } else if (ch != ',') {
            throw new RuntimeException("Unexpected '" + ch + "'" + " at index " + data.getIndex());
          }
        }
      }
      case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> skipNumber(data);
      case 't' -> skipExactString(data, "true");
      case 'f' -> skipExactString(data, "false");
      case 'n' -> skipExactString(data, "null");
      default -> throw new RuntimeException("Unexpected character '" + firstChar + "'" + " at index " + data.getIndex());
    }
  }

  private static void skipNumber(final StringIterator data) {
    // check for negative number
    final char firstChar = data.peek();
    if (firstChar == '-') {
      data.next();
    }

    // Skip the integer part of the number
    skipDigits(data);

    // Skip the fractional part of the number
    char next = data.peek();
    if (next == '.') {
      data.next();
      skipDigits(data);
    }

    // Skip the exponent
    next = data.peek();
    if (next == 'e' || next == 'E') {
      data.next();
      next = data.peek();
      if (next == '+' || next == '-') {
        data.next();
      }
      skipDigits(data);
    }
  }

  /**
   * Advance the iterator past the next closing curly brace, ignoring all key/value pairs. Assumes
   * that the iterator is pointing inside an object, immediately after a key/value pair. Throw an
   * exception if we reach the end of the string before finding a closing brace, or if we find a
   * different unexpected terminator (e.g. a closing square bracket).
   */
  private static void readToEndOfObject(final StringIterator data) {
    while (data.hasNext()) {
      skipWhitespace(data);
      final char ch = data.next();
      if (ch == '}') {
        return;
      } else if (ch == ',') {
        // advance past the next key/value pair
        skipValue(data);
        skipWhitespaceAndCharacter(data, ':');
        skipValue(data);
      } else {
        throw new RuntimeException("Unexpected '" + ch + "'" + " at index " + data.getIndex());
      }
    }
    throw new RuntimeException("Unexpected end of string");
  }

  private static void skipWhitespace(final StringIterator data) {
    while (data.hasNext()) {
      final char ch = data.peek();
      if (!Character.isWhitespace(ch)) {
        return;
      }
      data.next();
    }
  }

  private static void skipWhitespaceAndCharacter(final StringIterator data, final char ch) {
    skipWhitespace(data);
    final char actualCharacter = data.peek();
    if (actualCharacter == ch) {
      data.next();
    } else {
      throw new RuntimeException("Expected '" + ch + "'" + " at index " + data.getIndex() + " but got '" + actualCharacter + "'");
    }
  }

  private static void skipExactString(final StringIterator data, final String str) {
    for (int i = 0; i < str.length(); i++) {
      final char target = str.charAt(i);
      final char actualChar = data.next();
      if (actualChar != target) {
        throw new RuntimeException("Expected '" + target + "'" + " at index " + data.getIndex() + " but got '" + actualChar + "'");
      }
    }
  }

  /**
   * Skip characters until we find a non-numeric character
   */
  private static void skipDigits(final StringIterator data) {
    while (data.hasNext()) {
      final char ch = data.peek();
      if (ch < '0' || '9' < ch) {
        return;
      }
      data.next();
    }
  }

}