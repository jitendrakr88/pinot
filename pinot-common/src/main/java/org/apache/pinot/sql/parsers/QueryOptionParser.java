/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.sql.parsers;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryOptionParser {

    private static final int LENGTH_OF_OPTION = "option".length();
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryOptionParser.class);

    /** Half-open interval representing the option expression. */
    @Nullable private Pair<Integer, Integer> _optionIndex;

    private QueryOptionParser() {
    }

    /**
     * Takes in the SQL query with or without options, stores the options in the map that is passed,
     * and returns the SQL query without the options. We simply put each key-value pair to the map,
     * overwriting any existing values. Option expression can only exist at the end of the query.
     */
    public static String extractQueryOptions(String sql, Map<String, String> sink) {
        return new QueryOptionParser().extractQueryOptionsInternal(sql, sink);
    }

    /**
     * Removes query options from the sql, and returns the options in a map. Algorithm is:
     *
     * <ol>
     *   <li>Convert to lowercase
     *   <li>Find the last occurrence of the option keyword
     *   <li>If it exists, try to extract key-value pairs, and strip the option expression from the
     *       query
     * </ol>
     */
    private String extractQueryOptionsInternal(String sql, Map<String, String> sink) {
        String lowercaseSql = sql.toLowerCase(Locale.ENGLISH);
        int lastIndexOfOptionKeyword = lowercaseSql.lastIndexOf("option");
        if (lastIndexOfOptionKeyword == -1) {
            return sql;
        }
        try {
            Map<String, String> keyValuePairs =
                    extractKeyValuePairs(lowercaseSql, lastIndexOfOptionKeyword);
            sink.putAll(keyValuePairs);
        } catch (Throwable ignored) {
        }
        if (_optionIndex == null) {
            return sql;
        }
        return stripSubstring(sql, _optionIndex);
    }

    /**
     * Given a lower case query, and an index representing the start of the "option" keyword, extracts
     * all query option key-values that follow. This is O(n), and does allocations only for the option
     * key-value pairs.
     */
    private Map<String, String> extractKeyValuePairs(String lowercaseSql, int startIndexOfOption) {
        Map<String, String> result = new HashMap<>();
        // Jump past the "option" keyword
        int currentIndex = startIndexOfOption + LENGTH_OF_OPTION;
        currentIndex = consumeCharacter(lowercaseSql, currentIndex, '(');
        currentIndex = skipWhitespace(lowercaseSql, currentIndex);
        if (currentIndex < lowercaseSql.length() && lowercaseSql.charAt(currentIndex) == ')') {
            // Handles empty options: 'option()'
            // TODO: Clean this up after migrating any existing users.
            LOGGER.warn("[query-option-parser] Found empty option expression: 'option()'");
            _optionIndex = new Pair<>(startIndexOfOption, currentIndex + 1);
            return result;
        } else if (currentIndex < lowercaseSql.length() && lowercaseSql.charAt(currentIndex) == ',') {
            // Handles empty options with a single comma: 'option(,)'
            // TODO: Clean this up after migrating any existing users.
            LOGGER.warn("[query-option-parser] Found illegal empty option expression: 'option(,)'");
            currentIndex++;
            currentIndex = skipWhitespace(lowercaseSql, currentIndex);
            Preconditions.checkState(
                    currentIndex < lowercaseSql.length() && lowercaseSql.charAt(currentIndex) == ')');
            _optionIndex = new Pair<>(startIndexOfOption, currentIndex + 1);
            return result;
        }
        // loop-invariant: we progress by >3 characters in each iteration, because the consume methods
        // consume at least
        // 1 character.
        while (currentIndex < lowercaseSql.length()) {

            StringBuilder keyBuffer = new StringBuilder();
            currentIndex = consumeToken(lowercaseSql, keyBuffer, currentIndex);

            currentIndex = consumeCharacter(lowercaseSql, currentIndex, '=');

            StringBuilder valueBuffer = new StringBuilder();
            currentIndex = consumeToken(lowercaseSql, valueBuffer, currentIndex);

            currentIndex = skipWhitespace(lowercaseSql, currentIndex);

            Preconditions.checkState(currentIndex < lowercaseSql.length(), "Expected to read ')' or ','");
            Preconditions.checkState(keyBuffer.length() > 0, "Empty keys should not be possible");
            Preconditions.checkState(valueBuffer.length() > 0, "Empty values should not be possible");

            result.put(keyBuffer.toString(), valueBuffer.toString());

            if (lowercaseSql.charAt(currentIndex) == ',') {
                currentIndex++;
                // We skip whitespace and fall through to ')' check, because legacy implementation allowed
                // option(k=v,)
                skipWhitespace(lowercaseSql, currentIndex);
            }
            Preconditions.checkState(currentIndex < lowercaseSql.length(), "Expected to read ')' or ','");
            if (lowercaseSql.charAt(currentIndex) == ')') {
                currentIndex++;
                break;
            }
        }
        _optionIndex = new Pair<>(startIndexOfOption, currentIndex);
        return result;
    }

    /**
     * Starts at the given index in the input string, skips whitespaces, and returns the index if the
     * expected character is found. Returns the index after the expected character.
     *
     * <p><b>Invariant:</b> Either progresses by at least 1 character, or throws, terminating
     * execution.
     */
    static int consumeCharacter(String input, int currentIndex, char expectedCharacter) {
        currentIndex = skipWhitespace(input, currentIndex);
        if (currentIndex < input.length() && input.charAt(currentIndex) == expectedCharacter) {
            return ++currentIndex;
        }
        // No message required since this is to terminate execution
        throw new IllegalArgumentException();
    }

    /**
     * Starts at the given index in the input string, skips whitespaces, and consumes a key or value
     * token. Valid characters are: [a-zA-Z0-9_]. Returns the index after the last character of the
     * token.
     *
     * <p><b>Invariant:</b> Either progresses by at least 1 character, or throws, terminating
     * execution.
     */
    static int consumeToken(String input, StringBuilder tokenBuffer, int currentIndex) {
        currentIndex = skipWhitespace(input, currentIndex);
        if (isQuoteCharacter(input.charAt(currentIndex))) {
            return consumeQuotedToken(input, tokenBuffer, currentIndex);
        }
        return consumeUnquotedToken(input, tokenBuffer, currentIndex);
    }


    /**
     * Returns true if the character is a single or double quote.
     */
    static boolean isQuoteCharacter(char c) {
        return (c == '\'' || c == '"');
    }

    /**
     * Consumes a quoted token starting at the opening quote character.
     * Ensures the token ends with a matching quote.
     */
    static int consumeQuotedToken(String input, StringBuilder tokenBuffer, int currentIndex) {

        char quote = input.charAt(currentIndex);
        Preconditions.checkState(isQuoteCharacter(quote),
                "Expected the first character to be an opening quote character");

        tokenBuffer.append(quote);
        currentIndex++;

        while (currentIndex < input.length()
                && input.charAt(currentIndex) != quote
                && isValidQueryOptionCharacter(input.charAt(currentIndex))) {
            tokenBuffer.append(input.charAt(currentIndex));
            currentIndex++;
        }

        Preconditions.checkState(currentIndex < input.length() && input.charAt(currentIndex) == quote,
                "Expected input to end with the same quote character as it started with");

        tokenBuffer.append(quote);
        currentIndex++;

        return currentIndex;
    }

    /**
     * Consumes an unquoted token consisting of valid characters.
     * Fails if the token is empty.
     */
    static int consumeUnquotedToken(String input, StringBuilder tokenBuffer, int currentIndex) {
        while (currentIndex < input.length()
                && isValidQueryOptionCharacter(input.charAt(currentIndex))) {
            tokenBuffer.append(input.charAt(currentIndex));
            currentIndex++;
        }

        Preconditions.checkState(tokenBuffer.length() > 0, "Expected a non-empty token to consume.");
        return currentIndex;
    }

    /**
     * Starts at the given index in the input string, skips whitespaces, and returns index for the
     * first non-whitespace character.
     */
    static int skipWhitespace(String input, int currentIndex) {
        while (currentIndex < input.length() && Character.isWhitespace(input.charAt(currentIndex))) {
            currentIndex++;
        }
        return currentIndex;
    }

    /** Valid characters for query option keys and values are [a-zA-Z0-9_]. */
    static boolean isValidQueryOptionCharacter(char c) {
        return Character.isDigit(c) || Character.isAlphabetic(c) || c == '_';
    }

    static String stripSubstring(String input, Pair<Integer, Integer> interval) {
        StringBuilder result = new StringBuilder();
        result.append(input, 0, interval.getFirst());
        result.append(input, interval.getSecond(), input.length());
        return result.toString();
    }
}
