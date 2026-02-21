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

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestApiDateTimeUtils
{
    @Test
    public void testParseTimestampToEpochMicros()
    {
        long parsed = ApiDateTimeUtils.parseTimestampToEpochMicros("01/12/2025 07:23:11");
        long expected = LocalDateTime.of(2025, 12, 1, 7, 23, 11).toEpochSecond(ZoneOffset.UTC) * MICROSECONDS_PER_SECOND;
        assertThat(parsed).isEqualTo(expected);
    }

    @Test
    public void testExtractDateFromTrinoBound()
    {
        long epochMicros = LocalDateTime.of(2026, 1, 7, 9, 30, 0).toEpochSecond(ZoneOffset.UTC) * MICROSECONDS_PER_SECOND;
        assertThat(ApiDateTimeUtils.toLocalDateFromTrinoBound(epochMicros)).isEqualTo(LocalDate.of(2026, 1, 7));
    }
}
