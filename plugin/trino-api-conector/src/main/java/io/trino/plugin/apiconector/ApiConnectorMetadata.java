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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ApiConnectorMetadata
        implements ConnectorMetadata
{
    private final String schemaName;

    @Inject
    public ApiConnectorMetadata(ApiConnectorConfig config)
    {
        requireNonNull(config, "config is null");
        this.schemaName = config.getSchemaName().toLowerCase(ENGLISH);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(schemaName);
    }

    @Override
    public ApiTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (!schemaName.equals(tableName.getSchemaName().toLowerCase(ENGLISH))) {
            return null;
        }

        ApiTableDefinition table = ApiConnectorTableDefinitions.getTable(tableName.getTableName());
        if (table == null) {
            return null;
        }
        return new ApiTableHandle(schemaName, table.tableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ApiTableHandle handle = (ApiTableHandle) tableHandle;
        return getTableMetadata(handle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        if (optionalSchemaName.isPresent() && !schemaName.equals(optionalSchemaName.get().toLowerCase(ENGLISH))) {
            return ImmutableList.of();
        }

        return ApiConnectorTableDefinitions.listTables().stream()
                .map(table -> new SchemaTableName(schemaName, table.tableName()))
                .toList();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ApiTableHandle handle = (ApiTableHandle) tableHandle;
        ApiTableDefinition table = ApiConnectorTableDefinitions.getTable(handle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(handle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new ApiColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ApiColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ApiTableHandle current = (ApiTableHandle) table;
        TupleDomain<ColumnHandle> newConstraint = current.getConstraint().intersect(constraint.getSummary());

        Optional<String> constrainedStartDate = current.getStartDate();
        Optional<String> constrainedEndDate = current.getEndDate();

        ApiTableDefinition tableDefinition = ApiConnectorTableDefinitions.getTable(current.getTableName());
        if (tableDefinition == null) {
            throw new TableNotFoundException(current.toSchemaTableName());
        }

        Optional<DateRange> requestedRange = extractDateRange(newConstraint, tableDefinition.dateColumnName());
        if (requestedRange.isPresent()) {
            constrainedStartDate = mergeStartDate(constrainedStartDate, requestedRange.get().startDate());
            constrainedEndDate = mergeEndDate(constrainedEndDate, requestedRange.get().endDate());
        }

        ApiTableHandle updated = current.withConstraintAndDates(newConstraint, constrainedStartDate, constrainedEndDate);
        if (updated.equals(current)) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(updated, newConstraint, constraint.getExpression(), false));
    }

    private Optional<DateRange> extractDateRange(TupleDomain<ColumnHandle> constraint, String dateColumnName)
    {
        if (constraint.isNone()) {
            return Optional.empty();
        }

        Optional<Map<ColumnHandle, Domain>> domains = constraint.getDomains();
        if (domains.isEmpty()) {
            return Optional.empty();
        }

        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            ApiColumnHandle column = (ApiColumnHandle) entry.getKey();
            if (!column.getColumnName().equalsIgnoreCase(dateColumnName)) {
                continue;
            }

            Domain domain = entry.getValue();
            if (domain.getValues().isNone()) {
                return Optional.empty();
            }

            Range span = domain.getValues().getRanges().getSpan();
            Optional<String> startDate = span.isLowUnbounded()
                    ? Optional.empty()
                    : Optional.of(ApiDateTimeUtils.formatRequestDate(ApiDateTimeUtils.toLocalDateFromTrinoBound(span.getLowBoundedValue())));
            Optional<String> endDate = span.isHighUnbounded()
                    ? Optional.empty()
                    : Optional.of(ApiDateTimeUtils.formatRequestDate(ApiDateTimeUtils.toLocalDateFromTrinoBound(span.getHighBoundedValue())));
            return Optional.of(new DateRange(startDate, endDate));
        }
        return Optional.empty();
    }

    private static Optional<String> mergeStartDate(Optional<String> current, Optional<String> incoming)
    {
        if (incoming.isEmpty()) {
            return current;
        }
        if (current.isEmpty()) {
            return incoming;
        }

        LocalDate currentDate = ApiDateTimeUtils.parseRequestDate(current.get());
        LocalDate incomingDate = ApiDateTimeUtils.parseRequestDate(incoming.get());
        return Optional.of(ApiDateTimeUtils.formatRequestDate(incomingDate.isAfter(currentDate) ? incomingDate : currentDate));
    }

    private static Optional<String> mergeEndDate(Optional<String> current, Optional<String> incoming)
    {
        if (incoming.isEmpty()) {
            return current;
        }
        if (current.isEmpty()) {
            return incoming;
        }

        LocalDate currentDate = ApiDateTimeUtils.parseRequestDate(current.get());
        LocalDate incomingDate = ApiDateTimeUtils.parseRequestDate(incoming.get());
        return Optional.of(ApiDateTimeUtils.formatRequestDate(incomingDate.isBefore(currentDate) ? incomingDate : currentDate));
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!schemaName.equals(tableName.getSchemaName().toLowerCase(ENGLISH))) {
            return null;
        }

        ApiTableDefinition table = ApiConnectorTableDefinitions.getTable(tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    private record DateRange(Optional<String> startDate, Optional<String> endDate)
    {
        private DateRange
        {
            requireNonNull(startDate, "startDate is null");
            requireNonNull(endDate, "endDate is null");

            if (startDate.isPresent() && endDate.isPresent()) {
                LocalDate start = ApiDateTimeUtils.parseRequestDate(startDate.get());
                LocalDate end = ApiDateTimeUtils.parseRequestDate(endDate.get());
                if (start.isAfter(end)) {
                    throw new IllegalArgumentException(format("Faixa de data inválida: %s > %s", startDate.get(), endDate.get()));
                }
            }
        }
    }
}
