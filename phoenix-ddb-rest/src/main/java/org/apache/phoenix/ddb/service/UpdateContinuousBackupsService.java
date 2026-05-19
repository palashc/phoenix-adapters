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

package org.apache.phoenix.ddb.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.phoenix.ddb.service.utils.TableDescriptorUtils;
import org.apache.phoenix.ddb.utils.ApiMetadata;

/**
 * Service class for handling UpdateContinuousBackups API requests.
 * Always returns DISABLED status as Phoenix does not support continuous backups.
 * This is a no-op stub for KCL parity.
 */
public class UpdateContinuousBackupsService {

    public static Map<String, Object> updateContinuousBackups(Map<String, Object> request,
            String connectionUrl) {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);

        // Check if table exists - this will throw appropriate exception if table doesn't exist
        TableDescriptorUtils.getTableDescription(tableName, connectionUrl, "Table");

        // Create response with DISABLED continuous backups (no-op - backups are not supported)
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> continuousBackupsDescription = new HashMap<>();

        // Set continuous backups status to DISABLED
        continuousBackupsDescription.put("ContinuousBackupsStatus", "DISABLED");

        // Add point in time recovery description (also disabled)
        Map<String, Object> pointInTimeRecoveryDescription = new HashMap<>();
        pointInTimeRecoveryDescription.put("PointInTimeRecoveryStatus", "DISABLED");
        continuousBackupsDescription.put("PointInTimeRecoveryDescription",
                pointInTimeRecoveryDescription);

        response.put("ContinuousBackupsDescription", continuousBackupsDescription);

        return response;
    }
}
