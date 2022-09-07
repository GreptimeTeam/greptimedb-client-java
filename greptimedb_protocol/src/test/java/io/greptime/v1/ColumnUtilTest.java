package io.greptime.v1;

import com.google.protobuf.ByteStringHelper;
import io.greptime.models.ColumnUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;

/**
 *
 * @author jiachun.fjc
 */
public class ColumnUtilTest {

    @Test
    public void testGetValueSomeNull() {
        BitSet nullMask = new BitSet(5);
        nullMask.set(1, true);
        nullMask.set(3, true);
        Columns.Column column = Columns.Column.newBuilder()
            .setColumnName("test_column") //
            .setSemanticType(Columns.Column.SemanticType.FIELD) //
            .setValues(Columns.Column.Values.newBuilder() //
                .addI32Values(1) //
                .addI32Values(3) //
                .addI32Values(5) //
                .build())
            .setValueIndex(2) //
            .setNullMask(ByteStringHelper.wrap(nullMask.toByteArray())) //
            .build();

        Object v = ColumnUtil.getValue(column, 0, ColumnUtil.getNullMaskBits(column));
        Assert.assertEquals(1, v);
        v = ColumnUtil.getValue(column, 1, ColumnUtil.getNullMaskBits(column));
        Assert.assertNull(v);
        v = ColumnUtil.getValue(column, 2, ColumnUtil.getNullMaskBits(column));
        Assert.assertEquals(3, v);
        v = ColumnUtil.getValue(column, 3, ColumnUtil.getNullMaskBits(column));
        Assert.assertNull(v);
        v = ColumnUtil.getValue(column, 4, ColumnUtil.getNullMaskBits(column));
        Assert.assertEquals(5, v);
    }

    @Test
    public void testGetValueNonNull() {
        Columns.Column column = Columns.Column.newBuilder()
            .setColumnName("test_column") //
            .setSemanticType(Columns.Column.SemanticType.FIELD) //
            .setValues(Columns.Column.Values.newBuilder() //
                .addI32Values(1) //
                .addI32Values(2) //
                .addI32Values(3) //
                .build())
            .setValueIndex(2) //
            .build();

        Object v = ColumnUtil.getValue(column, 0, ColumnUtil.getNullMaskBits(column));
        Assert.assertEquals(1, v);
        v = ColumnUtil.getValue(column, 1, ColumnUtil.getNullMaskBits(column));
        Assert.assertEquals(2, v);
        v = ColumnUtil.getValue(column, 2, ColumnUtil.getNullMaskBits(column));
        Assert.assertEquals(3, v);
    }
}
