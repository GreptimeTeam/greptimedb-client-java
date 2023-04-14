/*
 * Copyright 2023 Greptime Team
 *
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
package io.greptime.models;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class ResultTest {

    @Test
    public void testMap() {
        final Result<WriteOk, Err> r1 = Result.ok(WriteOk.ok(2, 0, null));
        final Result<Integer, Err> r2 = r1.map(WriteOk::getSuccess);
        Assert.assertEquals(2, r2.getOk().intValue());

        final Result<WriteOk, Err> r5 = Result.err(Err.writeErr(400, null, null, null));
        final Result<Integer, Err> r6 = r5.map(WriteOk::getSuccess);
        Assert.assertFalse(r6.isOk());
    }

    @Test
    public void testMapOr() {
        final Result<WriteOk, Err> r1 = Result.ok(WriteOk.ok(2, 0, null));
        final Integer r2 = r1.mapOr(-1, WriteOk::getSuccess);
        Assert.assertEquals(2, r2.intValue());

        final Result<WriteOk, Err> r3 = Result.err(Err.writeErr(400, null, null, null));
        final Integer r4 = r3.mapOr(-1, WriteOk::getSuccess);
        Assert.assertEquals(-1, r4.intValue());
    }

    @Test
    public void testMapOrElse() {
        final Result<WriteOk, Err> r1 = Result.ok(WriteOk.ok(2, 0, null));
        final Integer r2 = r1.mapOrElse(err -> -1, WriteOk::getSuccess);
        Assert.assertEquals(2, r2.intValue());

        final Result<WriteOk, Err> r3 = Result.err(Err.writeErr(400, null, null, null));
        final Integer r4 = r3.mapOrElse(err -> -1, WriteOk::getSuccess);
        Assert.assertEquals(-1, r4.intValue());
    }

    @Test
    public void testMapErr() {
        final Result<WriteOk, Err> r1 = Result.ok(WriteOk.ok(2, 0, null));
        final Result<WriteOk, Throwable> r2 = r1.mapErr(Err::getError);
        Assert.assertEquals(2, r2.getOk().getSuccess());

        IllegalStateException error = new IllegalStateException("error test");
        final Result<WriteOk, Err> r3 = Result.err(Err.writeErr(400, error, null, null));
        final Result<WriteOk, Throwable> r4 = r3.mapErr(Err::getError);
        Assert.assertEquals("error test", r4.getErr().getMessage());
    }

    @Test
    public void testAndThen() {
        final Result<WriteOk, Err> r1 = Result.ok(WriteOk.ok(2, 0, null));
        final Result<WriteOk, Err> r2 = r1.andThen(writeOk -> {
            WriteOk newOne = WriteOk.ok(writeOk.getSuccess() + 1, 0, null);
            return newOne.mapToResult();
        });
        Assert.assertEquals(3, r2.getOk().getSuccess());

        final Result<WriteOk, Err> r3 = Result.err(Err.writeErr(400, null, null, null));
        final Result<WriteOk, Err> r4 = r3.andThen(writeOk -> {
            WriteOk newOne = WriteOk.ok(writeOk.getSuccess() + 1, 0, null);
            return newOne.mapToResult();
        });
        Assert.assertFalse(r4.isOk());
    }

    @Test
    public void testOrElse() {
        final Result<WriteOk, Err> r1 = Result.ok(WriteOk.ok(2, 0, null));
        final Result<WriteOk, String> r2 = r1.orElse(err -> Result.ok(WriteOk.ok(0, 0, null)));
        Assert.assertEquals(2, r2.getOk().getSuccess());

        final Result<WriteOk, Err> r3 = Result.err(Err.writeErr(400, null, null, null));
        final Result<WriteOk, String> r4 = r3.orElse(err -> Result.ok(WriteOk.ok(0, 0, null)));
        Assert.assertEquals(0, r4.getOk().getSuccess());
    }

    @Test
    public void testUnwrapOr() {
        final Result<WriteOk, Err> r1 = Result.ok(WriteOk.ok(2, 0, null));
        final WriteOk r2 = r1.unwrapOr(WriteOk.emptyOk());
        Assert.assertEquals(2, r2.getSuccess());

        final Result<WriteOk, Err> r3 = Result.err(Err.writeErr(400, null, null, null));
        final WriteOk r4 = r3.unwrapOr(WriteOk.emptyOk());
        Assert.assertEquals(0, r4.getSuccess());
    }

    @Test
    public void testUnwrapOrElse() {
        final Result<WriteOk, Err> r1 = Result.ok(WriteOk.ok(2, 0, null));
        final WriteOk r2 = r1.unwrapOrElse(err -> WriteOk.emptyOk());
        Assert.assertEquals(2, r2.getSuccess());

        final Result<WriteOk, Err> r3 = Result.err(Err.writeErr(400, null, null, null));
        final WriteOk r4 = r3.unwrapOrElse(err -> WriteOk.emptyOk());
        Assert.assertEquals(0, r4.getSuccess());
    }
}
