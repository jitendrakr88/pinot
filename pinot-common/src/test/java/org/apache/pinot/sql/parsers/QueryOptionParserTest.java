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

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.math3.util.Pair;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class QueryOptionParserTest {

    @Test
    public void testQueryOptionParserWhenQueryChanged() {
        testQueryOption(
            "SELECT * FROM t OPTION (a = b, c= d)",
            "SELECT * FROM t ",
            ImmutableMap.of("a", "b", "c", "d"));

        testQueryOption(
            "SELECT * FROM t OPTION (a = b, c= d) OPTION (e = f)",
            "SELECT * FROM t OPTION (a = b, c= d) ",
            ImmutableMap.of("e", "f"));

        testQueryOption(
            "SELECT * FROM t opTion(a=b,c=d) -- trailing comment",
            "SELECT * FROM t  -- trailing comment",
            ImmutableMap.of("a", "b", "c", "d"));

        testQueryOption("SELECT * from t option() ", "SELECT * from t  ", ImmutableMap.of());

        testQueryOption(
            "SELECT * from t option() option() ", "SELECT * from t option()  ", ImmutableMap.of());

        testQueryOption("SELECT * from t option(,) ", "SELECT * from t  ", ImmutableMap.of());

        // Current code still applies this query option, which is incorrect. We keep the same behavior
        // for now.
        testQueryOption("-- option(timeoutMs=1000)", "-- ", ImmutableMap.of("timeoutms", "1000"));

        testQueryOption("実 回צץ option (timeoutms=   1000)", "実 回צץ ", ImmutableMap.of("timeoutms", "1000"));

        testQueryOption(
            "SELECT * FROM rta_menu_item_option WHERE foo = 'bar' option(timeoutms=1000)",
            "SELECT * FROM rta_menu_item_option WHERE foo = 'bar' ",
            ImmutableMap.of("timeoutms", "1000"));

        testQueryOption("SELECT * FROM t option(foo=\"bar\")", "SELECT * FROM t ", ImmutableMap.of("foo", "\"bar\""));
        testQueryOption("SELECT * FROM t option(foo='bar')", "SELECT * FROM t ", ImmutableMap.of("foo", "'bar'"));
    }

    @Test
    public void testQueryOptionParserWhenQueryUnchanged() {
        testQueryOptionWhenQueryUnchanged("SELECT * FROM t option (opt1=true,opt2!false,opt3=true)");
        testQueryOptionWhenQueryUnchanged("SELECT * FROM t option (axy)");
        testQueryOptionWhenQueryUnchanged("SELECT * FROM t ");
        testQueryOptionWhenQueryUnchanged("SELECT * FROM rta_menu_item_option WHERE foo = 'bar'");
        testQueryOptionWhenQueryUnchanged(
                "SELECT * FROM t OPTION (a = b, timeoutms= true) OPTION(opt1=true,opt2!false,opt3=true)");
        testQueryOptionWhenQueryUnchanged(
                "SELECT * FROM option(key=value) rta_menu_item_option WHERE foo = 'bar'");
    }

    @Test
    public void testStripSubstring() {
        // Test with easy to read and validate cases. Each case strips half-open interval indices from
        // the main string.
        assertEquals(QueryOptionParser.stripSubstring("01234567", Pair.create(2, 4)), "014567");
        assertEquals(QueryOptionParser.stripSubstring("01234567", Pair.create(0, 4)), "4567");
        assertEquals(QueryOptionParser.stripSubstring("01234567", Pair.create(0, 8)), "");
        assertEquals(QueryOptionParser.stripSubstring("01234567", Pair.create(0, 0)), "01234567");
        assertEquals(QueryOptionParser.stripSubstring("01234567", Pair.create(7, 7)), "01234567");
        assertEquals(QueryOptionParser.stripSubstring("01234567", Pair.create(7, 8)), "0123456");
        // Test with query like string
        assertEquals(
                QueryOptionParser.stripSubstring("SELECT * FROM t OPTION(a=b)", Pair.create(16, 27)),
                "SELECT * FROM t ");
    }

    private void testQueryOptionWhenQueryUnchanged(String input) {
        // Invalid query option expr will be ignored.
        Map<String, String> queryOptionSink = new HashMap<>();
        assertEquals(QueryOptionParser.extractQueryOptions(input, queryOptionSink, true), input);
        assertTrue(queryOptionSink.isEmpty());
    }

    private void testQueryOption(
            String input, String expectedQueryOutput, Map<String, String> expectedMap) {
        Map<String, String> options = new HashMap<>();
        assertEquals(QueryOptionParser.extractQueryOptions(input, options, true), expectedQueryOutput);
        assertEquals(options, expectedMap);
    }
}
