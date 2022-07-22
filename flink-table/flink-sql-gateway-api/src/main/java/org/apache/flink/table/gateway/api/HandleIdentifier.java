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

package org.apache.flink.table.gateway.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.gateway.api.utils.ParseTools;

import java.util.Objects;
import java.util.UUID;

/** Identifiers for Handle. */
@PublicEvolving
public class HandleIdentifier {

    private final UUID publicId;
    private final UUID secretId;

    public HandleIdentifier(UUID publicId, UUID secretId) {
        this.publicId = publicId;
        this.secretId = secretId;
    }

    public String getFullID() {
        return String.format(
                "%s%s%s", publicId.toString(), ParseTools.SEPARATOR, secretId.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HandleIdentifier)) {
            return false;
        }
        HandleIdentifier that = (HandleIdentifier) o;
        return Objects.equals(publicId, that.publicId) && Objects.equals(secretId, that.secretId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(publicId, secretId);
    }

    @Override
    public String toString() {
        return publicId.toString();
    }
}
