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

import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.MappedForwardingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.SentinelRow;
import io.crate.execution.engine.collect.CollectExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * BatchIterator that computes an aggregate or window function against a window over the source batch iterator.
 * Executing a function over a window means having to range over a set of rows from the source iterator. By default (ie.
 * empty window) the range is between unbounded preceding and current row. This means the function will need to be
 * computed against all the rows from the beginning of the partition and through the current row's _last peer_. The
 * function result will be emitted for every row in the window.
 * <p>
 *      For eg.
 * <p>
 *      source : [ 1, 2, 3, 4 ]
 * <p>
 *      function : sum
 * <p>
 *      window definition : empty (equivalent to an empty OVER() clause)
 * <p>
 *      window range is all the rows in the source as all rows are peers if there is no ORDER BY in the OVER clause
 * <p>
 *      Execution steps :
 * <p>
 *          1. compute the function while the rows are peers
 * <p>
 *                  1 + 2 + 3 + 4 = 10
 * <p>
 *          2. for each row in the window emit the function result
 * <p>
 *                  [ 10, 10, 10, 10 ]
 */
public class WindowBatchIterator extends MappedForwardingBatchIterator<Row, Row> {

    private final BatchIterator<Row> source;
    private final List<WindowFunction> functions;
    private final WindowDefinition windowDefinition;
    private final Object[] outgoingCells;
    private final LinkedList<Object[]> standaloneOutgoingCells;
    private final LinkedList<Object[]> resultsForCurrentFrame;
    private final List<CollectExpression<Row, ?>> standaloneExpressions;

    private Row currentWindowRow;
    /**
     * Represents the "window row", the row for which we are computing the window, and once that's complete, execute the
     * window function for.
     */
    private Object[] currentRowCells = null;


    private int sourceRowsConsumed;
    private int windowRowPosition;
    private final List<Row> windowForCurrentRow = new ArrayList<>();
    private boolean foundCurrentRowsLastPeer = false;
    private int windowFuncColumnsCount;

    WindowBatchIterator(WindowDefinition windowDefinition,
                        List<Input<?>> standaloneInputs,
                        List<CollectExpression<Row, ?>> standaloneExpressions,
                        BatchIterator<Row> source,
                        List<WindowFunction> functions) {
        assert windowDefinition.partitions().size() == 0 : "Window partitions are not supported.";
        assert windowDefinition.windowFrameDefinition() == null : "Window frame definitions are not supported";

        this.windowDefinition = windowDefinition;
        this.source = source;
        this.standaloneExpressions = standaloneExpressions;
        for (WindowFunction function : functions) {
            windowFuncColumnsCount += function.numColumns();
        }
        this.outgoingCells = new Object[windowFuncColumnsCount + standaloneInputs.size()];
        this.standaloneOutgoingCells = new LinkedList<>();
        this.resultsForCurrentFrame = new LinkedList<>();
        this.functions = functions;
    }

    private boolean arePeers(Object[] prevRowCells, Object[] currentRowCells) {
        OrderBy orderBy = windowDefinition.orderBy();
        if (orderBy == null) {
            // all rows are peers when orderBy is missing
            return true;
        }

        return prevRowCells == currentRowCells ||
               Arrays.equals(prevRowCells, currentRowCells);
    }

    @Override
    protected BatchIterator<Row> delegate() {
        return source;
    }

    @Override
    public Row currentElement() {
        return currentWindowRow;
    }

    @Override
    public void moveToStart() {
        super.moveToStart();
        sourceRowsConsumed = 0;
        windowRowPosition = 0;
        windowForCurrentRow.clear();
        currentRowCells = null;
        currentWindowRow = null;
    }

    @Override
    public boolean moveNext() {
        if (foundCurrentRowsLastPeer && windowRowPosition < sourceRowsConsumed - 1) {
            // emit the result of the window function as we computed the result and not yet emitted it for every row
            // in the window
            windowRowPosition++;
            computeCurrentElement();
            return true;
        }

        while (source.moveNext()) {
            sourceRowsConsumed++;
            Row currentSourceRow = source.currentElement();
            computeAndFillStandaloneOutgoingCellsFor(currentSourceRow);
            Object[] sourceRowCells = currentSourceRow.materialize();
            if (sourceRowsConsumed == 1) {
                // first row in the source is the "current window row" we start with
                currentRowCells = sourceRowCells;
            }

            if (arePeers(currentRowCells, sourceRowCells)) {
                windowForCurrentRow.add(new RowN(sourceRowCells));
                foundCurrentRowsLastPeer = false;
            } else {
                foundCurrentRowsLastPeer = true;

                executeWindowFunctions();
                // on the next source iteration, we'll start building the window for the next window row
                currentRowCells = sourceRowCells;
                windowForCurrentRow.add(new RowN(currentRowCells));
                windowRowPosition++;
                computeCurrentElement();
                return true;
            }
        }

        if (source.allLoaded()) {
            if (!windowForCurrentRow.isEmpty()) {
                // we're done with consuming the source iterator, but were still in the process of building up the
                // window for the current window row. As there are no more rows to process, execute the function against
                // what we currently accumulated in the window and start yielding the result.
                executeWindowFunctions();
            }

            if (windowRowPosition < sourceRowsConsumed) {
                // we still need to emit rows
                windowRowPosition++;
                computeCurrentElement();
                return true;
            }
        }
        return false;
    }

    private void computeCurrentElement() {
        if (resultsForCurrentFrame.size() > 0) {
            Object[] windowFunctionsResult = resultsForCurrentFrame.removeFirst();
            System.arraycopy(windowFunctionsResult, 0, outgoingCells, 0, windowFunctionsResult.length);
        }
        if (standaloneOutgoingCells.size() > 0) {
            Object[] inputRowCells = standaloneOutgoingCells.removeFirst();
            System.arraycopy(inputRowCells, 0, outgoingCells, windowFuncColumnsCount, inputRowCells.length);
        }
        currentWindowRow = new RowN(outgoingCells);
    }

    private void computeAndFillStandaloneOutgoingCellsFor(Row sourceRow) {
        if (standaloneExpressions.size() > 0) {
            Object[] standaloneInputValues = new Object[standaloneExpressions.size()];
            for (int i = 0; i < standaloneExpressions.size(); i++) {
                CollectExpression<Row, ?> expression = standaloneExpressions.get(i);
                expression.setNextRow(sourceRow);
                standaloneInputValues[i] = expression.value();
            }
            standaloneOutgoingCells.add(standaloneInputValues);
        }
    }

    private void executeWindowFunctions() {
        WindowFrame currentFrame = new WindowFrame(
            windowRowPosition,
            sourceRowsConsumed,
            InMemoryBatchIterator.of(windowForCurrentRow, SentinelRow.SENTINEL)
        );

        Object[][] cellsForCurrentFrame = executeFunctionsAndUnnestResults(currentFrame);
        for (Object[] outgoingCells : cellsForCurrentFrame) {
            resultsForCurrentFrame.push(outgoingCells);
        }

        windowForCurrentRow.clear();
    }

    private Object[][] executeFunctionsAndUnnestResults(WindowFrame frame) {
        int frameSize = windowForCurrentRow.size();
        Object[][] cellsForCurrentFrame = new Object[frameSize][windowFuncColumnsCount];

        int filledNumColumns = 0;
        for (WindowFunction function : functions) {
            Iterator<Row> resultsIterator = function.execute(frame).iterator();
            // some functions will return only one result (eg. aggregates) but when used in conjunction with functions
            // that return multiple results (eg. row_number) we will fill in the result returned by the aggregate for
            // all the rows in the frame
            Object[] lastResult = null;
            for (int i = 0; i < frameSize; i++) {
                if (resultsIterator.hasNext()) {
                    Object[] resultCells = resultsIterator.next().materialize();
                    lastResult = resultCells;
                    System.arraycopy(resultCells, 0, cellsForCurrentFrame[i], filledNumColumns, resultCells.length);
                } else {
                    System.arraycopy(lastResult, 0, cellsForCurrentFrame[i], filledNumColumns, lastResult.length);
                }
            }
            filledNumColumns += function.numColumns();
            frame.moveToStart();
        }
        return cellsForCurrentFrame;
    }
}
