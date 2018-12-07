/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.window;

import io.crate.analyze.WindowDefinition;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class WindowBatchIteratorTest {

    private List<Object[]> expectedRowNumberResult = IntStream.range(0, 10)
        .mapToObj(l -> new Object[]{l + 1}).collect(Collectors.toList());

    @Test
    public void testSumWindowBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new WindowBatchIterator(
                emptyWindow(),
                Collections.emptyList(),
                Collections.emptyList(),
                TestingBatchIterators.range(0, 10),
                Collections.singletonList(getRowNumberWindowFunction())
            )
        );

        tester.verifyResultAndEdgeCaseBehaviour(expectedRowNumberResult);
    }

    @Test
    public void testSumWindowBatchIteratorWithBatchSimulatingSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new WindowBatchIterator(
                emptyWindow(),
                Collections.emptyList(),
                Collections.emptyList(),
                new BatchSimulatingIterator<>(
                    TestingBatchIterators.range(0, 10), 4, 2, null),
                Collections.singletonList(getRowNumberWindowFunction())
            )
        );

        tester.verifyResultAndEdgeCaseBehaviour(expectedRowNumberResult);
    }

    private WindowFunction getRowNumberWindowFunction() {
        return new WindowFunction() {
            @Override
            public Iterable<Row> execute(WindowFrame frame) {
                ArrayList<Row> rows = new ArrayList<>();
                while (frame.moveNext()) {
                    rows.add(new Row1(frame.windowRowPosition()));
                }
                return rows;
            }

            @Override
            public int numColumns() {
                return 1;
            }
        };
    }

    @Test
    public void testFrameBounds() throws Exception {
        WindowFunction frameBoundsWindowFunction = new WindowFunction() {
            @Override
            public Iterable<Row> execute(WindowFrame frame) {
                return Collections.singletonList(new RowN(new Object[]{frame.frameStartPosition(), frame.frameEndPosition()}));
            }

            @Override
            public int numColumns() {
                return 2;
            }
        };

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(new WindowBatchIterator(
            emptyWindow(),
            Collections.emptyList(),
            Collections.emptyList(),
            TestingBatchIterators.range(0, 10),
            Collections.singletonList(frameBoundsWindowFunction)), null);

        assertThat(consumer.getResult().get(0), is(Collections.singletonList(new Object[]{0, 10})));
    }

    private static WindowDefinition emptyWindow() {
        return new WindowDefinition(Collections.emptyList(), null, null);
    }

}
