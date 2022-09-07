package io.greptime.v1;

import com.google.protobuf.Descriptors;
import io.greptime.common.util.Ensures;
import io.greptime.models.Value;

import java.util.BitSet;

/**
 * A utility that handles some processing of column data.
 *
 * @author jiachun.fjc
 */
public final class ColumnUtil {

    private static final String[] COLUMN_VALUE_NAME_INDEX;
    private static final Value.Type[] COLUMN_VALUE_TYPE_INDEX;

    static {
        COLUMN_VALUE_NAME_INDEX = new String[13];
        COLUMN_VALUE_NAME_INDEX[0] = "i8_values";
        COLUMN_VALUE_NAME_INDEX[1] = "i16_values";
        COLUMN_VALUE_NAME_INDEX[2] = "i32_values";
        COLUMN_VALUE_NAME_INDEX[3] = "i64_values";
        COLUMN_VALUE_NAME_INDEX[4] = "u8_values";
        COLUMN_VALUE_NAME_INDEX[5] = "u16_values";
        COLUMN_VALUE_NAME_INDEX[6] = "u32_values";
        COLUMN_VALUE_NAME_INDEX[7] = "u64_values";
        COLUMN_VALUE_NAME_INDEX[8] = "f32_values";
        COLUMN_VALUE_NAME_INDEX[9] = "f64_values";
        COLUMN_VALUE_NAME_INDEX[10] = "bool_values";
        COLUMN_VALUE_NAME_INDEX[11] = "binary_values";
        COLUMN_VALUE_NAME_INDEX[12] = "string_values";

        COLUMN_VALUE_TYPE_INDEX = new Value.Type[13];
        COLUMN_VALUE_TYPE_INDEX[0] = Value.Type.Int32;
        COLUMN_VALUE_TYPE_INDEX[1] = Value.Type.Int32;
        COLUMN_VALUE_TYPE_INDEX[2] = Value.Type.Int32;
        COLUMN_VALUE_TYPE_INDEX[3] = Value.Type.Int64;
        COLUMN_VALUE_TYPE_INDEX[4] = Value.Type.UInt32;
        COLUMN_VALUE_TYPE_INDEX[5] = Value.Type.UInt32;
        COLUMN_VALUE_TYPE_INDEX[6] = Value.Type.UInt32;
        COLUMN_VALUE_TYPE_INDEX[7] = Value.Type.UInt64;
        COLUMN_VALUE_TYPE_INDEX[8] = Value.Type.Float;
        COLUMN_VALUE_TYPE_INDEX[9] = Value.Type.Double;
        COLUMN_VALUE_TYPE_INDEX[10] = Value.Type.Bool;
        COLUMN_VALUE_TYPE_INDEX[11] = Value.Type.Bytes;
        COLUMN_VALUE_TYPE_INDEX[12] = Value.Type.String;
    }

    public static Value.Type getValueType(Columns.Column column) {
        int index = column.getValueIndex();
        Ensures.ensure(index < COLUMN_VALUE_TYPE_INDEX.length, "value_index overflow: %d", index);
        return COLUMN_VALUE_TYPE_INDEX[index];
    }

    public static Object getValue(Columns.Column column, int cursor, BitSet nullMask) {
        int index = column.getValueIndex();
        Ensures.ensure(index < COLUMN_VALUE_NAME_INDEX.length, "value_index overflow: %d", index);
        String fieldName = COLUMN_VALUE_NAME_INDEX[index];
        Columns.Column.Values values = column.getValues();
        Descriptors.FieldDescriptor fd = values.getDescriptorForType().findFieldByName(fieldName);
        if (nullMask.isEmpty()) {
            return values.getRepeatedField(fd, cursor);
        }

        Ensures.ensure(cursor < nullMask.size());

        int dataIndex = 0;
        for(int i = 0; i <= cursor; i++) {
            if (!nullMask.get(i)) {
                dataIndex++;
            }
        }
        return values.getRepeatedField(fd, dataIndex);
    }

    private ColumnUtil() {
    }
}
