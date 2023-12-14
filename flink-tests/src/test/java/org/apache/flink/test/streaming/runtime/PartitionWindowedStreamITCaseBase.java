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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.streaming.api.datastream.PartitionWindowedStream;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The base IT case for {@link PartitionWindowedStream}. */
public abstract class PartitionWindowedStreamITCaseBase {

    protected void expectInAnyOrder(CloseableIterator<String> resultIterator, String... expected) {
        List<String> listExpected = Lists.newArrayList(expected);
        List<String> testResults = Lists.newArrayList(resultIterator);
        Collections.sort(listExpected);
        Collections.sort(testResults);
        assertEquals(listExpected, testResults);
    }

    /** The test pojo. */
    public static class TestPojo {

        public String key;

        public Integer value;

        public TestPojo() {}

        public TestPojo(Integer value) {
            this.value = value;
        }

        public TestPojo(String key, Integer value) {
            this.key = key;
            this.value = value;
        }

        public Integer getValue() {
            return value;
        }

        public void setValue(Integer value) {
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }

    /** The test accumulator. */
    public static class TestAccumulator {
        private Integer testField = 100;

        public void addTestField(Integer number) {
            testField = testField - number;
        }

        public String getTestField() {
            return String.valueOf(testField);
        }
    }
}
