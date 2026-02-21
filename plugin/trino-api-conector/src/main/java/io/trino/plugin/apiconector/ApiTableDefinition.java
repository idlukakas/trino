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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record ApiTableDefinition(
        String tableName,
        String endpointName,
        String dateColumnName,
        List<ApiColumnDefinition> columns)
{
    public ApiTableDefinition
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(endpointName, "endpointName is null");
        requireNonNull(dateColumnName, "dateColumnName is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columns.stream()
                .map(ApiColumnDefinition::toColumnMetadata)
                .toList();
    }

    public record ApiColumnDefinition(String name, Type type)
    {
        public ApiColumnDefinition
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
        }

        public ColumnMetadata toColumnMetadata()
        {
            return new ColumnMetadata(name, type);
        }
    }
}
