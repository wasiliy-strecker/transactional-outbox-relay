package io.github.wasiliystrecker.outbox.core;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

final class Validation {
  private static final int MAX_HEADER_COUNT = 64;
  private static final int MAX_HEADER_NAME_LENGTH = 128;
  private static final int MAX_HEADER_VALUE_LENGTH = 2_048;

  private Validation() {}

  static String text(String value, String name, int maxLength) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    if (value.length() > maxLength) {
      throw new IllegalArgumentException(name + " must not exceed " + maxLength + " characters");
    }
    return value;
  }

  static <T> T required(T value, String name) {
    return Objects.requireNonNull(value, name + " must not be null");
  }

  static Map<String, String> headers(Map<String, String> source) {
    required(source, "headers");
    if (source.size() > MAX_HEADER_COUNT) {
      throw new IllegalArgumentException("headers must not contain more than 64 entries");
    }
    Map<String, String> copy = new LinkedHashMap<>();
    source.forEach(
        (key, value) -> {
          String checkedKey = text(key, "header name", MAX_HEADER_NAME_LENGTH);
          if (checkedKey.toLowerCase(Locale.ROOT).startsWith("outbox.")) {
            throw new IllegalArgumentException("header names beginning with outbox. are reserved");
          }
          copy.put(checkedKey, text(value, "header value", MAX_HEADER_VALUE_LENGTH));
        });
    return Map.copyOf(copy);
  }
}
