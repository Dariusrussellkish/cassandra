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

package org.apache.cassandra.db.causalreader;

public class CausalUtility
{
    // Tunable Parameter
    private static final int writerID = 0;
    private static final int num_nodes = 3;


    // Number that specify your column
    // also should be the schema of our table
    private static final String col_prefix = "vcol";
    private static final String local_col_prefix = "local";
    private static final String myTimeCol = col_prefix + writerID;
    private static final String senderCol = "sendfrom";

    // Get the col I need to mutate when doing mutation
    public static String getMyTimeColName() {
        return myTimeCol;
    }

    // Get the col I need to mutate when doing mutation
    public static String getColPrefix() {
        return col_prefix;
    }

    public static int getWriterID () {
        return writerID;
    }

    public static int getNumNodes() {
        return num_nodes;
    }

    public static String getSenderColName() {
        return senderCol;
    }

    public static String getLocalColPrefix() {
        return local_col_prefix;
    }





}
