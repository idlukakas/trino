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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ApiRecordSet
        implements RecordSet
{
    private final List<ApiColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final List<Map<String, Object>> rows;

    public ApiRecordSet(List<ApiColumnHandle> columnHandles, List<Map<String, Object>> rows)
    {
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.columnTypes = this.columnHandles.stream()
                .map(ApiColumnHandle::getColumnType)
                .toList();
        this.rows = ImmutableList.copyOf(requireNonNull(rows, "rows is null"));
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new ApiRecordCursor(columnHandles, rows);
    }
}
