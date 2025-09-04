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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for DISTINCT_COUNT_BITMAP64 and DISTINCT_COUNT_BITMAP64MV aggregation functions.
 * This test validates that the 64-bit bitmap functions work correctly in a full cluster environment.
 */
public class DistinctCountBitmap64IntegrationTest extends BaseClusterIntegrationTest {

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Test
  public void testDistinctCountBitmap64BasicFunctionality() throws Exception {
    // Test basic DISTINCT_COUNT_BITMAP64 functionality
    String query = "SELECT DISTINCTCOUNTBITMAP64(DaysSinceEpoch) FROM mytable";
    JsonNode response = postQuery(query);
    
    // Should return a positive count (expected ~364 based on dataset)
    long result = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(result > 0, "DISTINCT_COUNT_BITMAP64 should return positive count");
    assertTrue(result < 1000, "Result should be reasonable for test dataset");
  }

  @Test
  public void testDistinctCountBitmap64VsBitmap32Consistency() throws Exception {
    // Compare DISTINCT_COUNT_BITMAP64 with DISTINCT_COUNT_BITMAP for same data
    String query32 = "SELECT DISTINCTCOUNTBITMAP(DaysSinceEpoch) FROM mytable";
    String query64 = "SELECT DISTINCTCOUNTBITMAP64(DaysSinceEpoch) FROM mytable";
    
    JsonNode response32 = postQuery(query32);
    JsonNode response64 = postQuery(query64);
    
    long result32 = response32.get("resultTable").get("rows").get(0).get(0).asLong();
    long result64 = response64.get("resultTable").get("rows").get(0).get(0).asLong();
    
    // Results should be identical for the same dataset
    assertEquals(result64, result32, "64-bit and 32-bit bitmap functions should return same result for same data");
  }

  @Test
  public void testDistinctCountBitmap64MultiValue() throws Exception {
    // Test DISTINCT_COUNT_BITMAP64MV functionality
    String query = "SELECT DISTINCTCOUNTBITMAP64MV(DivAirportIDs) FROM mytable";
    JsonNode response = postQuery(query);
    
    // Should return a positive count for multi-value column
    long result = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(result > 0, "DISTINCT_COUNT_BITMAP64MV should return positive count");
    assertTrue(result < 1000, "MV result should be reasonable for test dataset");
  }

  @Test
  public void testDistinctCountBitmap64MVVsBitmapMVConsistency() throws Exception {
    // Compare DISTINCT_COUNT_BITMAP64MV with DISTINCT_COUNT_BITMAPMV
    String queryMV32 = "SELECT DISTINCTCOUNTBITMAPMV(DivAirportIDs) FROM mytable";
    String queryMV64 = "SELECT DISTINCTCOUNTBITMAP64MV(DivAirportIDs) FROM mytable";
    
    JsonNode responseMV32 = postQuery(queryMV32);
    JsonNode responseMV64 = postQuery(queryMV64);
    
    long resultMV32 = responseMV32.get("resultTable").get("rows").get(0).get(0).asLong();
    long resultMV64 = responseMV64.get("resultTable").get("rows").get(0).get(0).asLong();
    
    // Results should be identical for multi-value columns
    assertEquals(resultMV64, resultMV32, "64-bit and 32-bit MV bitmap functions should return same result");
  }

  @Test
  public void testDistinctCountBitmap64WithGroupBy() throws Exception {
    // Test GROUP BY functionality with DISTINCT_COUNT_BITMAP64
    String query = "SELECT Origin, DISTINCTCOUNTBITMAP64(DaysSinceEpoch) FROM mytable GROUP BY Origin LIMIT 10";
    JsonNode response = postQuery(query);
    
    JsonNode rows = response.get("resultTable").get("rows");
    assertTrue(rows.size() > 0, "GROUP BY query should return results");
    
    // Each group should have reasonable distinct counts
    for (int i = 0; i < Math.min(rows.size(), 5); i++) {
      JsonNode row = rows.get(i);
      String origin = row.get(0).asText();
      long distinctCount = row.get(1).asLong();
      
      assertTrue(distinctCount > 0, "Each group should have positive distinct count for origin: " + origin);
      assertTrue(distinctCount <= 400, "Distinct count should be reasonable for origin: " + origin);
    }
  }

  @Test
  public void testDistinctCountBitmap64WithFilters() throws Exception {
    // Test with WHERE clause
    String query = "SELECT DISTINCTCOUNTBITMAP64(DaysSinceEpoch) FROM mytable WHERE ArrDelay > 0";
    JsonNode response = postQuery(query);
    
    long result = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(result > 0, "Filtered query should return positive count");
    
    // Compare with unfiltered result
    String unfilteredQuery = "SELECT DISTINCTCOUNTBITMAP64(DaysSinceEpoch) FROM mytable";
    JsonNode unfilteredResponse = postQuery(unfilteredQuery);
    long unfilteredResult = unfilteredResponse.get("resultTable").get("rows").get(0).get(0).asLong();
    
    assertTrue(result <= unfilteredResult, "Filtered result should be <= unfiltered result");
  }

  @Test
  public void testDistinctCountBitmap64WithDifferentDataTypes() throws Exception {
    // Test with different numeric columns to verify hash handling
    List<String> numericColumns = Arrays.asList("ArrDelay", "DepDelay", "Distance", "DaysSinceEpoch");
    
    for (String column : numericColumns) {
      String query = "SELECT DISTINCTCOUNTBITMAP64(" + column + ") FROM mytable";
      JsonNode response = postQuery(query);
      
      long result = response.get("resultTable").get("rows").get(0).get(0).asLong();
      assertTrue(result > 0, "DISTINCT_COUNT_BITMAP64 should work for column: " + column);
    }
  }

  @Test
  public void testDistinctCountBitmap64WithStringColumns() throws Exception {
    // Test with string columns (hash-based distinct counting)
    List<String> stringColumns = Arrays.asList("Origin", "Dest", "UniqueCarrier");
    
    for (String column : stringColumns) {
      String query = "SELECT DISTINCTCOUNTBITMAP64(" + column + ") FROM mytable";
      JsonNode response = postQuery(query);
      
      long result = response.get("resultTable").get("rows").get(0).get(0).asLong();
      assertTrue(result > 0, "DISTINCT_COUNT_BITMAP64 should work for string column: " + column);
      
      // Compare with regular distinct count for validation
      String validateQuery = "SELECT DISTINCTCOUNT(" + column + ") FROM mytable";
      JsonNode validateResponse = postQuery(validateQuery);
      long validateResult = validateResponse.get("resultTable").get("rows").get(0).get(0).asLong();
      
      assertEquals(result, validateResult, 
          "DISTINCT_COUNT_BITMAP64 should match DISTINCTCOUNT for column: " + column);
    }
  }

  @Test
  public void testDistinctCountBitmap64Performance() throws Exception {
    // Test that 64-bit version performs reasonably
    long startTime = System.currentTimeMillis();
    
    String query = "SELECT DISTINCTCOUNTBITMAP64(DaysSinceEpoch) FROM mytable";
    JsonNode response = postQuery(query);
    
    long endTime = System.currentTimeMillis();
    long queryTime = endTime - startTime;
    
    // Should complete within reasonable time (5 seconds for test data)
    assertTrue(queryTime < 5000, "Query should complete within 5 seconds, took: " + queryTime + "ms");
    
    // Verify result is still correct
    long result = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(result > 0, "Performance test should still return correct result");
  }

  @Override
  protected String getSchemaFileName() {
    return DEFAULT_SCHEMA_FILE_NAME;
  }

  @AfterClass
  public void tearDown() throws Exception {
    // Clean up test data
    dropOfflineTable(DEFAULT_TABLE_NAME);
    
    // Stop the cluster
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    
    // Clean up directories
    FileUtils.deleteDirectory(_tempDir);
  }


}
