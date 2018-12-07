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

import io.crate.data.BatchIterator;
import io.crate.data.MappedForwardingBatchIterator;
import io.crate.data.Row;

public class WindowFrame extends MappedForwardingBatchIterator<Row, Row> {

    private int windowRowPosition;
    private final int frameStartPosition;
    private final int frameEndPosition;
    private final BatchIterator<Row> rows;

    public WindowFrame(int frameStartPositionInclusive, int frameEndPositionExclusive, BatchIterator<Row> rows) {
        this.frameStartPosition = frameStartPositionInclusive;
        this.frameEndPosition = frameEndPositionExclusive;
        this.windowRowPosition = frameStartPositionInclusive;
        this.rows = rows;
    }

    @Override
    public boolean moveNext() {
        boolean iteratorMoved = super.moveNext();
        if (iteratorMoved) {
            windowRowPosition++;
        }
        return iteratorMoved;
    }

    @Override
    protected BatchIterator<Row> delegate() {
        return rows;
    }

    int windowRowPosition() {
        return windowRowPosition;
    }

    int frameStartPosition() {
        return frameStartPosition;
    }

    int frameEndPosition() {
        return frameEndPosition;
    }

    @Override
    public void moveToStart() {
        super.moveToStart();
        windowRowPosition = frameStartPosition;
    }

    @Override
    public Row currentElement() {
        return rows.currentElement();
    }
}
