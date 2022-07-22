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

package org.apache.flink.table.gateway.api.utils;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.table.gateway.api.HandleIdentifier;
import org.apache.flink.table.gateway.api.session.SessionHandle;

import java.util.UUID;

/** Tools for parse path parameters. */
public class ParseTools {

    public static final String SEPARATOR = "|";
    public static final String SEPARATOR_REGEX = "\\|";

    public static SessionHandle parseStringToSessinHandler(String sessionHandleId)
            throws ConversionException {
        String[] ids;
        UUID publicId;
        UUID secretId;
        try {
            ids = sessionHandleId.split(SEPARATOR_REGEX);
            publicId = UUID.fromString(ids[0]);
            secretId = UUID.fromString(ids[1]);
        } catch (Exception e) {
            throw new ConversionException(
                    "The id of SessionHandler should contain both publicId and secretId.");
        }
        return new SessionHandle(new HandleIdentifier(publicId, secretId));
    }
}
