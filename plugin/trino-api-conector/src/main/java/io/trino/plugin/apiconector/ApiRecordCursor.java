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
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class ApiRecordCursor
        implements RecordCursor
{
    private final List<ApiColumnHandle> columnHandles;
    private final List<Map<String, Object>> rows;

    private int currentPosition = -1;
    private Map<String, Object> currentRow;

    public ApiRecordCursor(List<ApiColumnHandle> columnHandles, List<Map<String, Object>> rows)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.rows = requireNonNull(rows, "rows is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        currentPosition++;
        if (currentPosition >= rows.size()) {
            currentRow = null;
            return false;
        }
        currentRow = rows.get(currentPosition);
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        Object value = getFieldValue(field);
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof Number number) {
            return number.longValue() != 0;
        }
        String text = normalizeString(value);
        if ("1".equals(text)) {
            return true;
        }
        if ("0".equals(text)) {
            return false;
        }
        return Boolean.parseBoolean(text);
    }

    @Override
    public long getLong(int field)
    {
        Type type = getType(field);
        checkArgument(type.getJavaType() == long.class, "Expected field %s to map to long but found %s", field, type);

        Object value = getFieldValue(field);
        if (type instanceof TimestampType) {
            return ApiDateTimeUtils.parseTimestampToEpochMicros(value);
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof Boolean bool) {
            return bool ? 1L : 0L;
        }

        String text = normalizeString(value);
        try {
            return Long.parseLong(text);
        }
        catch (NumberFormatException ignored) {
            return new BigDecimal(text).longValue();
        }
    }

    @Override
    public double getDouble(int field)
    {
        Object value = getFieldValue(field);
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return Double.parseDouble(normalizeString(value));
    }

    @Override
    public Slice getSlice(int field)
    {
        Type type = getType(field);
        checkArgument(type instanceof VarcharType, "Expected field %s to be VARCHAR but is %s", field, type);
        Object value = getFieldValue(field);
        if (value == null) {
            return Slices.utf8Slice("");
        }
        return Slices.utf8Slice(String.valueOf(value));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException("Object type is not supported");
    }

    @Override
    public boolean isNull(int field)
    {
        Type type = getType(field);
        Object value = getNullableFieldValue(field);
        if (value == null) {
            return true;
        }
        return !(type instanceof VarcharType) && value instanceof String stringValue && stringValue.isBlank();
    }

    @Override
    public void close() {}

    private Object getFieldValue(int field)
    {
        Object value = getNullableFieldValue(field);
        checkState(value != null, "Field is null. Check isNull() before reading value.");
        return value;
    }

    private Object getNullableFieldValue(int field)
    {
        checkState(currentRow != null, "Cursor has not been advanced yet");
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return currentRow.get(columnHandles.get(field).getColumnName());
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be %s but was %s", field, expected, actual);
    }

    private static String normalizeString(Object value)
    {
        String text = String.valueOf(requireNonNull(value, "value is null")).trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException("Valor vazio não pode ser convertido");
        }
        return text;
    }
}
