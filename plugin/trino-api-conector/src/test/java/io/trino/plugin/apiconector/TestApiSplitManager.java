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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestApiSplitManager
{
    @Test
    public void testSplitChunkingForThirtyOneDays()
    {
        List<ApiSplit> splits = ApiSplitManager.buildSplitsForRange(
                "lista-lead",
                LocalDate.of(2025, 1, 1),
                LocalDate.of(2025, 2, 15),
                31);

        assertThat(splits).hasSize(2);
        assertThat(splits.get(0).getStartDate()).isEqualTo("2025-01-01");
        assertThat(splits.get(0).getEndDate()).isEqualTo("2025-01-31");
        assertThat(splits.get(1).getStartDate()).isEqualTo("2025-02-01");
        assertThat(splits.get(1).getEndDate()).isEqualTo("2025-02-15");
    }
}
