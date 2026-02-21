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

import io.airlift.slice.Slice;
import io.trino.spi.type.LongTimestamp;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

public final class ApiDateTimeUtils
{
    private static final List<DateTimeFormatter> SUPPORTED_API_TIMESTAMP_FORMATS = List.of(
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm", Locale.ENGLISH),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH));

    private ApiDateTimeUtils() {}

    public static String formatRequestDate(LocalDate date)
    {
        return DateTimeFormatter.ISO_LOCAL_DATE.format(date);
    }

    public static LocalDate parseRequestDate(String value)
    {
        return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
    }

    public static LocalDate toLocalDateFromTrinoBound(Object value)
    {
        if (value instanceof Long epochMicros) {
            return fromEpochMicros(epochMicros).toLocalDate();
        }
        if (value instanceof LongTimestamp timestamp) {
            return fromEpochMicros(timestamp.getEpochMicros()).toLocalDate();
        }
        if (value instanceof LocalDate localDate) {
            return localDate;
        }
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime.toLocalDate();
        }
        if (value instanceof Slice slice) {
            return parseApiTimestamp(slice.toStringUtf8()).toLocalDate();
        }
        throw new IllegalArgumentException("Tipo de bound de data não suportado: " + value.getClass().getName());
    }

    public static long parseTimestampToEpochMicros(Object value)
    {
        if (value instanceof Number number) {
            return normalizeEpochNumber(number.longValue());
        }
        if (value instanceof String stringValue) {
            LocalDateTime parsed = parseApiTimestamp(stringValue);
            return toEpochMicros(parsed);
        }
        throw new IllegalArgumentException("Tipo de data/hora não suportado: " + value.getClass().getName());
    }

    private static long normalizeEpochNumber(long value)
    {
        long absolute = Math.abs(value);
        if (absolute >= 1_000_000_000_000_000L) {
            return value;
        }
        if (absolute >= 1_000_000_000_000L) {
            return value * 1_000;
        }
        return value * MICROSECONDS_PER_SECOND;
    }

    private static LocalDateTime parseApiTimestamp(String value)
    {
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Valor de data/hora vazio");
        }

        for (DateTimeFormatter formatter : SUPPORTED_API_TIMESTAMP_FORMATS) {
            try {
                return LocalDateTime.parse(normalized, formatter);
            }
            catch (DateTimeParseException ignored) {
                // tenta próximo formato
            }
        }

        try {
            return LocalDate.parse(normalized, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay();
        }
        catch (DateTimeParseException ignored) {
            // tenta formato com timezone a seguir
        }

        Instant instant = Instant.parse(normalized);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static long toEpochMicros(LocalDateTime localDateTime)
    {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * MICROSECONDS_PER_SECOND
                + (localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND);
    }

    private static LocalDateTime fromEpochMicros(long epochMicros)
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int nanoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
        Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
