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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ApiSplitManager
        implements ConnectorSplitManager
{
    private final ApiConnectorConfig config;

    @Inject
    public ApiSplitManager(ApiConnectorConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        ApiTableHandle tableHandle = (ApiTableHandle) connectorTableHandle;
        ApiTableDefinition table = ApiConnectorTableDefinitions.getTable(tableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }

        if (tableHandle.getConstraint().isNone()) {
            return new FixedSplitSource(ImmutableList.of());
        }

        LocalDate endDate = resolveEndDate(tableHandle.getEndDate(), config.getDefaultEndDate());
        LocalDate startDate = resolveStartDate(tableHandle.getStartDate(), config.getDefaultStartDate(), endDate, config.getDefaultLookbackDays());

        if (startDate.isAfter(endDate)) {
            return new FixedSplitSource(ImmutableList.of());
        }

        List<ApiSplit> splits = buildSplitsForRange(table.endpointName(), startDate, endDate, config.getMaxDaysPerRequest());
        return new FixedSplitSource(ImmutableList.copyOf(splits));
    }

    static List<ApiSplit> buildSplitsForRange(String endpointName, LocalDate startDate, LocalDate endDate, int maxDaysPerRequest)
    {
        ImmutableList.Builder<ApiSplit> splits = ImmutableList.builder();
        LocalDate currentStart = startDate;

        while (!currentStart.isAfter(endDate)) {
            LocalDate currentEnd = currentStart.plusDays(maxDaysPerRequest - 1L);
            if (currentEnd.isAfter(endDate)) {
                currentEnd = endDate;
            }

            splits.add(new ApiSplit(
                    endpointName,
                    ApiDateTimeUtils.formatRequestDate(currentStart),
                    ApiDateTimeUtils.formatRequestDate(currentEnd)));

            currentStart = currentEnd.plusDays(1);
        }

        return splits.build();
    }

    private static LocalDate resolveStartDate(Optional<String> filteredStartDate, Optional<LocalDate> configuredStartDate, LocalDate endDate, int defaultLookbackDays)
    {
        if (filteredStartDate.isPresent()) {
            return ApiDateTimeUtils.parseRequestDate(filteredStartDate.get());
        }
        if (configuredStartDate.isPresent()) {
            return configuredStartDate.get();
        }
        return endDate.minusDays(defaultLookbackDays - 1L);
    }

    private static LocalDate resolveEndDate(Optional<String> filteredEndDate, Optional<LocalDate> configuredEndDate)
    {
        if (filteredEndDate.isPresent()) {
            return ApiDateTimeUtils.parseRequestDate(filteredEndDate.get());
        }
        if (configuredEndDate.isPresent()) {
            return configuredEndDate.get();
        }
        return LocalDate.now(ZoneOffset.UTC);
    }
}
