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
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.TableNotFoundException;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ApiRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final ApiConnectorClient client;

    @Inject
    public ApiRecordSetProvider(ApiConnectorClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        ApiSplit apiSplit = (ApiSplit) split;
        ApiTableHandle tableHandle = (ApiTableHandle) table;

        ApiTableDefinition tableDefinition = ApiConnectorTableDefinitions.getTable(tableHandle.getTableName());
        if (tableDefinition == null) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }

        ImmutableList.Builder<ApiColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            handles.add((ApiColumnHandle) column);
        }

        List<ApiColumnHandle> selectedColumns = handles.build();
        List<java.util.Map<String, Object>> rows = client.fetchRows(tableDefinition, apiSplit.getStartDate(), apiSplit.getEndDate());
        return new ApiRecordSet(selectedColumns, rows);
    }
}
