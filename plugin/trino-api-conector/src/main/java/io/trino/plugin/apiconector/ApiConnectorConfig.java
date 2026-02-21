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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ApiConnectorConfig
{
    private String baseUrl = "https://crm.api.une.cx";
    private String authCode;
    private String schemaName = "univesp";
    private Optional<LocalDate> defaultStartDate = Optional.empty();
    private Optional<LocalDate> defaultEndDate = Optional.empty();
    private int defaultLookbackDays = 7;
    private int maxDaysPerRequest = 31;
    private Duration connectTimeout = new Duration(10, TimeUnit.SECONDS);
    private Duration readTimeout = new Duration(60, TimeUnit.SECONDS);
    private Duration tokenRefreshSkew = new Duration(5, TimeUnit.MINUTES);
    private int maxRetries = 2;

    @NotNull
    public String getBaseUrl()
    {
        return baseUrl;
    }

    @Config("api.base-url")
    @ConfigDescription("Base URL da API Une/Univesp")
    public ApiConnectorConfig setBaseUrl(String baseUrl)
    {
        this.baseUrl = baseUrl;
        return this;
    }

    @NotNull
    public String getAuthCode()
    {
        return authCode;
    }

    @Config("api.auth-code")
    @ConfigDescription("Código para obtenção de token JWT (query param code)")
    public ApiConnectorConfig setAuthCode(String authCode)
    {
        this.authCode = authCode;
        return this;
    }

    @NotNull
    public String getSchemaName()
    {
        return schemaName;
    }

    @Config("api.schema-name")
    @ConfigDescription("Nome do schema no Trino")
    public ApiConnectorConfig setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
        return this;
    }

    @NotNull
    public Optional<LocalDate> getDefaultStartDate()
    {
        return defaultStartDate;
    }

    @Config("api.default-start-date")
    @ConfigDescription("Data padrão inicial no formato YYYY-MM-DD")
    public ApiConnectorConfig setDefaultStartDate(String defaultStartDate)
    {
        this.defaultStartDate = parseDate(defaultStartDate);
        return this;
    }

    @NotNull
    public Optional<LocalDate> getDefaultEndDate()
    {
        return defaultEndDate;
    }

    @Config("api.default-end-date")
    @ConfigDescription("Data padrão final no formato YYYY-MM-DD")
    public ApiConnectorConfig setDefaultEndDate(String defaultEndDate)
    {
        this.defaultEndDate = parseDate(defaultEndDate);
        return this;
    }

    @Min(1)
    public int getDefaultLookbackDays()
    {
        return defaultLookbackDays;
    }

    @Config("api.default-lookback-days")
    @ConfigDescription("Janela padrão em dias quando não há filtro de data")
    public ApiConnectorConfig setDefaultLookbackDays(int defaultLookbackDays)
    {
        this.defaultLookbackDays = defaultLookbackDays;
        return this;
    }

    @Min(1)
    public int getMaxDaysPerRequest()
    {
        return maxDaysPerRequest;
    }

    @Config("api.max-days-per-request")
    @ConfigDescription("Máximo de dias por requisição (a API suporta até 31)")
    public ApiConnectorConfig setMaxDaysPerRequest(int maxDaysPerRequest)
    {
        this.maxDaysPerRequest = maxDaysPerRequest;
        return this;
    }

    @NotNull
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("api.connect-timeout")
    @ConfigDescription("Timeout de conexão HTTP")
    public ApiConnectorConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @NotNull
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("api.read-timeout")
    @ConfigDescription("Timeout de leitura HTTP")
    public ApiConnectorConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }

    @NotNull
    public Duration getTokenRefreshSkew()
    {
        return tokenRefreshSkew;
    }

    @Config("api.token-refresh-skew")
    @ConfigDescription("Antecedência para renovação do token JWT")
    public ApiConnectorConfig setTokenRefreshSkew(Duration tokenRefreshSkew)
    {
        this.tokenRefreshSkew = tokenRefreshSkew;
        return this;
    }

    @Min(0)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("api.max-retries")
    @ConfigDescription("Quantidade máxima de tentativas de retry para falhas transitórias")
    public ApiConnectorConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    @AssertTrue(message = "api.max-days-per-request deve ser <= 31")
    public boolean isMaxDaysPerRequestValid()
    {
        return maxDaysPerRequest <= 31;
    }

    @AssertTrue(message = "api.default-start-date deve ser menor ou igual a api.default-end-date")
    public boolean isDefaultDateRangeValid()
    {
        return defaultStartDate.isEmpty() || defaultEndDate.isEmpty() || !defaultStartDate.get().isAfter(defaultEndDate.get());
    }

    private static Optional<LocalDate> parseDate(String value)
    {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(LocalDate.parse(value));
        }
        catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Data inválida '" + value + "'. Use o formato YYYY-MM-DD.", e);
        }
    }
}
