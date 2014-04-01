/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.node.dml;

import io.crate.analyze.CopyAnalysis;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;

public class CopyNode extends DMLPlanNode {


    private final CopyAnalysis.Mode mode;
    private final Symbol uri;
    private final String index;

    public CopyNode(Symbol uri, String index, CopyAnalysis.Mode mode) {
        this.uri = uri;
        this.index = index;
        this.mode = mode;
    }

    public Symbol uri() {
        return uri;
    }

    public String uriAsString(){
        // TODO: this method should not be needed anymore, once copy from does not use inout anymore
        return ((Literal) uri).valueAsString();
    }

    public String index() {
        return index;
    }

    public CopyAnalysis.Mode mode() {
        return mode;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitCopyNode(this, context);
    }
}
