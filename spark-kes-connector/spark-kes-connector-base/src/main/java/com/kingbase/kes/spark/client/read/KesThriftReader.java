// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.kingbase.kes.spark.client.read;

import com.kingbase.kes.spark.client.entity.KesReaderPartition;
import com.kingbase.kes.spark.rest.models.Schema;
import com.kingbase.kes.spark.util.SchemaConvertors;
import scala.collection.JavaConverters;

public class KesThriftReader extends AbstractThriftReader {

    public KesThriftReader(KesReaderPartition partition) throws Exception {
        super(partition);
    }

    @Override
    protected Schema getKesSchema() {
        return SchemaConvertors.convertToSchema(
                JavaConverters.asScalaBufferConverter(scanOpenResult.getSelectedColumns()).asScala().toSeq());
    }

}