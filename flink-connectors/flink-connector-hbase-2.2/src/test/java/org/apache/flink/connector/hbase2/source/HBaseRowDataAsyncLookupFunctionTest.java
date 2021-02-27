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

package org.apache.flink.connector.hbase2.source;

import org.apache.flink.connector.hbase.options.HBaseLookupOptions;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase.util.PlannerType;
import org.apache.flink.connector.hbase2.util.HBaseTestBase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Test suite for {@link HBaseRowDataAsyncLookupFunction}. */
public class HBaseRowDataAsyncLookupFunctionTest extends HBaseTestBase {

    @Override
    protected PlannerType planner() {
        // lookup table source is only supported in blink planner
        return PlannerType.BLINK_PLANNER;
    }

    @Test
    public void testEval() throws Exception {
        HBaseRowDataAsyncLookupFunction lookupFunction = buildRowDataAsyncLookupFunction();

        lookupFunction.open(null);
        List<RowData> list = new ArrayList<>();
        int[] rowKey = {1, 2, 3};
        for (int i = 0; i < rowKey.length; i++){
            CompletableFuture<Collection<RowData>> feature = new CompletableFuture<>();
            lookupFunction.eval(feature, rowKey[i]);
            list.add(feature.get().iterator().next());
        }
        lookupFunction.close();
        List<String> result =
            Lists.newArrayList(list).stream()
                .map(RowData::toString)
                .sorted()
                .collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("+I(1,+I(10),+I(Hello-1,100),+I(1.01,false,Welt-1))");
        expected.add("+I(2,+I(20),+I(Hello-2,200),+I(2.02,true,Welt-2))");
        expected.add("+I(3,+I(30),+I(Hello-3,300),+I(3.03,false,Welt-3))");
        assertEquals(expected, result);
    }

    private HBaseRowDataAsyncLookupFunction buildRowDataAsyncLookupFunction() {
        HBaseLookupOptions lookupOptions = HBaseLookupOptions.builder().build();
        TableSchema schema =
            TableSchema.builder()
                .field(ROW_KEY, DataTypes.INT())
                .field(FAMILY1, DataTypes.ROW(DataTypes.FIELD(F1COL1, DataTypes.INT())))
                .field(
                    FAMILY2,
                    DataTypes.ROW(
                        DataTypes.FIELD(F2COL1, DataTypes.STRING()),
                        DataTypes.FIELD(F2COL2, DataTypes.BIGINT())))
                .field(
                    FAMILY3,
                    DataTypes.ROW(
                        DataTypes.FIELD(F3COL1, DataTypes.DOUBLE()),
                        DataTypes.FIELD(F3COL2, DataTypes.BOOLEAN()),
                        DataTypes.FIELD(F3COL3, DataTypes.STRING())))
                .build();
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(schema);
        return new HBaseRowDataAsyncLookupFunction(
            getConf(),
            TEST_TABLE_1,
            hbaseSchema,
            "null",
            lookupOptions);
    }

}
