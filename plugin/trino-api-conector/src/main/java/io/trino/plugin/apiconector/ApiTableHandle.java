/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.apiconector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class ApiTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<String> startDate;
    private final Optional<String> endDate;

    @JsonCreator
    public ApiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("startDate") Optional<String> startDate,
            @JsonProperty("endDate") Optional<String> endDate)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.startDate = requireNonNull(startDate, "startDate is null");
        this.endDate = requireNonNull(endDate, "endDate is null");
    }

    public ApiTableHandle(String schemaName, String tableName)
    {
        this(schemaName, tableName, TupleDomain.all(), Optional.empty(), Optional.empty());
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<String> getStartDate()
    {
        return startDate;
    }

    @JsonProperty
    public Optional<String> getEndDate()
    {
        return endDate;
    }

    public ApiTableHandle withConstraintAndDates(TupleDomain<ColumnHandle> newConstraint, Optional<String> newStartDate, Optional<String> newEndDate)
    {
        return new ApiTableHandle(schemaName, tableName, newConstraint, newStartDate, newEndDate);
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, constraint, startDate, endDate);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ApiTableHandle other = (ApiTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.constraint, other.constraint) &&
                Objects.equals(this.startDate, other.startDate) &&
                Objects.equals(this.endDate, other.endDate);
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName + " startDate=" + startDate.orElse("<default>") + " endDate=" + endDate.orElse("<default>");
    }
}
