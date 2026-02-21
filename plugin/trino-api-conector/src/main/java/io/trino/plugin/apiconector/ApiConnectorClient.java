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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;
import static java.net.http.HttpRequest.BodyPublishers;
import static java.net.http.HttpResponse.BodyHandlers;
import static java.util.Objects.requireNonNull;

public class ApiConnectorClient
{
    private static final String AUTH_PATH = "/api/auth/univesp/key?code=%s";
    private static final String UNIVESP_ENDPOINT_TEMPLATE = "/api/univesp/%s";
    private static final long TOKEN_VALIDITY_SECONDS = 3600;

    private final ApiConnectorConfig config;
    private final HttpClient httpClient;
    private final Object tokenRefreshLock = new Object();

    private volatile CachedToken cachedToken;

    @Inject
    public ApiConnectorClient(ApiConnectorConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofMillis(config.getConnectTimeout().toMillis()))
                .build();
    }

    public List<Map<String, Object>> fetchRows(ApiTableDefinition table, String startDate, String endDate)
    {
        requireNonNull(table, "table is null");
        requireNonNull(startDate, "startDate is null");
        requireNonNull(endDate, "endDate is null");

        Map<String, String> payload = ImmutableMap.of(
                "data_inicio", startDate,
                "data_termino", endDate);

        JSONObject response = requestEndpointData(table.endpointName(), payload);
        JSONArray data = response.optJSONArray("data");
        if (data == null) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<Map<String, Object>> rows = ImmutableList.builder();
        for (int index = 0; index < data.length(); index++) {
            Object row = data.opt(index);
            if (!(row instanceof JSONObject rowNode)) {
                continue;
            }
            rows.add(normalizeRow(rowNode));
        }
        return rows.build();
    }

    private JSONObject requestEndpointData(String endpointName, Map<String, String> payload)
    {
        String requestBody = new JSONObject(payload).toString();

        URI endpointUri = buildUri(format(UNIVESP_ENDPOINT_TEMPLATE, endpointName));

        for (int authAttempt = 0; authAttempt < 2; authAttempt++) {
            String token = getValidToken(authAttempt == 1);
            HttpRequest request = HttpRequest.newBuilder(endpointUri)
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .timeout(java.time.Duration.ofMillis(config.getReadTimeout().toMillis()))
                    .POST(BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = sendWithRetries(request);
            if (response.statusCode() == 401) {
                invalidateCachedToken();
                continue;
            }

            JSONObject responseBody = parseJsonObject(response.body());
            if (isInvalidTokenResponse(responseBody)) {
                invalidateCachedToken();
                continue;
            }

            if (response.statusCode() / 100 != 2) {
                String message = responseBody.optString("message", "Falha ao consultar endpoint");
                throw new TrinoException(
                        GENERIC_USER_ERROR,
                        format("Erro ao consultar endpoint '%s' (HTTP %s): %s", endpointName, response.statusCode(), message));
            }

            if (!responseBody.optBoolean("success", false)) {
                String message = responseBody.optString("message", "API retornou sucesso=false");
                throw new TrinoException(
                        GENERIC_USER_ERROR,
                        format("Endpoint '%s' retornou erro: %s", endpointName, message));
            }
            return responseBody;
        }

        throw new TrinoException(GENERIC_USER_ERROR, "Não foi possível autenticar na API após renovar o token");
    }

    private HttpResponse<String> sendWithRetries(HttpRequest request)
    {
        int maxAttempts = config.getMaxRetries() + 1;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (response.statusCode() >= 500 && attempt < maxAttempts - 1) {
                    sleepBackoff(attempt);
                    continue;
                }
                return response;
            }
            catch (IOException e) {
                if (attempt >= maxAttempts - 1) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Falha de comunicação com API", e);
                }
                sleepBackoff(attempt);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Thread interrompida durante chamada HTTP", e);
            }
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Falha inesperada durante chamada HTTP");
    }

    private String getValidToken(boolean forceRefresh)
    {
        CachedToken currentToken = cachedToken;
        Instant now = Instant.now();
        if (!forceRefresh && currentToken != null && now.isBefore(currentToken.expiresAt())) {
            return currentToken.token();
        }

        synchronized (tokenRefreshLock) {
            CachedToken cached = cachedToken;
            Instant refreshNow = Instant.now();
            if (!forceRefresh && cached != null && refreshNow.isBefore(cached.expiresAt())) {
                return cached.token();
            }

            String token = fetchNewToken();
            Instant expiration = refreshNow
                    .plusSeconds(TOKEN_VALIDITY_SECONDS)
                    .minusMillis(config.getTokenRefreshSkew().toMillis());
            cachedToken = new CachedToken(token, expiration);
            return token;
        }
    }

    private void invalidateCachedToken()
    {
        cachedToken = null;
    }

    private String fetchNewToken()
    {
        URI authUri = buildUri(format(
                AUTH_PATH,
                URLEncoder.encode(config.getAuthCode(), StandardCharsets.UTF_8)));

        int maxAttempts = config.getMaxRetries() + 1;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) authUri.toURL().openConnection();
                connection.setRequestMethod("POST");
                connection.setConnectTimeout((int) config.getConnectTimeout().toMillis());
                connection.setReadTimeout((int) config.getReadTimeout().toMillis());
                connection.setDoOutput(true);
                connection.setFixedLengthStreamingMode(0L);
                connection.connect();
                try (OutputStream ignored = connection.getOutputStream()) {
                    // Forca envio do header Content-Length: 0
                }

                int statusCode = connection.getResponseCode();
                String responseBody = readResponseBody(connection);

                if (statusCode >= 500 && attempt < maxAttempts - 1) {
                    sleepBackoff(attempt);
                    continue;
                }

                if (statusCode / 100 != 2) {
                    throw new TrinoException(
                            GENERIC_USER_ERROR,
                            format("Falha ao obter token JWT (HTTP %s)", statusCode));
                }

                JSONObject payload = parseJsonObject(responseBody);
                String token = payload.optString("token", "");
                if (token.isBlank()) {
                    throw new TrinoException(GENERIC_USER_ERROR, "Resposta de autenticação sem token");
                }
                return token;
            }
            catch (IOException e) {
                if (attempt >= maxAttempts - 1) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Falha de comunicação ao obter token JWT", e);
                }
                sleepBackoff(attempt);
            }
            finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Falha inesperada ao obter token JWT");
    }

    private static String readResponseBody(HttpURLConnection connection)
            throws IOException
    {
        InputStream responseStream = connection.getResponseCode() >= 400 ? connection.getErrorStream() : connection.getInputStream();
        if (responseStream == null) {
            return "";
        }
        try (InputStream input = responseStream) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private JSONObject parseJsonObject(String body)
    {
        try {
            return new JSONObject(body);
        }
        catch (JSONException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Resposta JSON inválida da API", e);
        }
    }

    private boolean isInvalidTokenResponse(JSONObject payload)
    {
        if (payload.optBoolean("success", true)) {
            return false;
        }

        String message = payload.optString("message", "").toLowerCase(Locale.ENGLISH);
        return message.contains("token") && (message.contains("expir") || message.contains("invál") || message.contains("invalid"));
    }

    private Map<String, Object> normalizeRow(JSONObject rowNode)
    {
        Map<String, Object> normalized = new HashMap<>();
        for (String key : rowNode.keySet()) {
            normalized.put(
                    key.toLowerCase(Locale.ENGLISH),
                    toJavaValue(rowNode.opt(key)));
        }
        normalized.put("raw_json", rowNode.toString());

        return ImmutableMap.copyOf(normalized);
    }

    private static Object toJavaValue(Object value)
    {
        if (value == null || value == JSONObject.NULL) {
            return null;
        }
        if (value instanceof Boolean || value instanceof Number || value instanceof String) {
            return value;
        }
        if (value instanceof JSONObject || value instanceof JSONArray) {
            return value.toString();
        }
        return String.valueOf(value);
    }

    private URI buildUri(String path)
    {
        String base = config.getBaseUrl();
        if (base.endsWith("/") && path.startsWith("/")) {
            return URI.create(base + path.substring(1));
        }
        if (!base.endsWith("/") && !path.startsWith("/")) {
            return URI.create(base + "/" + path);
        }
        return URI.create(base + path);
    }

    private static void sleepBackoff(int attempt)
    {
        long delayMillis = Math.min(2_000L, 200L * (1L << attempt));
        try {
            Thread.sleep(delayMillis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Thread interrompida durante backoff", e);
        }
    }

    private record CachedToken(String token, Instant expiresAt)
    {
        private CachedToken
        {
            requireNonNull(token, "token is null");
            requireNonNull(expiresAt, "expiresAt is null");
        }
    }
}
