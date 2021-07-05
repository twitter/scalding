package com.twitter.scalding.parquet.convert;

import static com.twitter.scalding.parquet.convert.ParquetTimestampUtils.asTimestamp;
import static com.twitter.scalding.parquet.convert.ParquetTimestampUtils.getTimestampMillis;
import static org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils.getNanoTime;
import static org.apache.parquet.io.api.Binary.fromConstantByteBuffer;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

public class ParquetTimestampUtilsTest {

  @Test
  public void testGetTimestampMillis() {
    assertTimestampCorrect("2011-01-01 00:00:00.000000000");
    assertTimestampCorrect("2001-01-01 01:01:01.000000001");
    assertTimestampCorrect("2015-12-31 23:59:59.999999999");
  }

  @Test
  public void testInvalidBinaryLength() {
    try {
      byte[] invalidLengthBinaryTimestamp = new byte[8];
      getTimestampMillis(Binary.fromByteArray(invalidLengthBinaryTimestamp));
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Parquet timestamp must be 12 bytes, actual 8");
    }
  }

  @Test
  public void testGetTimestamp() {
    assertTimestampNanosCorrect("2011-01-01 00:00:00.0");
    assertTimestampNanosCorrect("2001-01-01 01:01:01.000000001");
    assertTimestampNanosCorrect("2015-12-31 23:59:59.999999999");
    assertTimestampNanosCorrect("2015-12-17 16:08:01.987654321");
  }

  private static void assertTimestampCorrect(String timestampString) {
    Timestamp timestamp = Timestamp.valueOf(timestampString);
    NanoTime nanoTime = getNanoTime(timestamp, false);
    ByteBuffer buffer = ByteBuffer.wrap(nanoTime.toBinary().getBytes());
    long decodedTimestampMillis = ParquetTimestampUtils
        .getTimestampMillis(fromConstantByteBuffer(buffer));
    assertEquals(decodedTimestampMillis, timestamp.getTime());
  }

  private static void assertTimestampNanosCorrect(String timestampString) {
    Timestamp timestamp = Timestamp.valueOf(timestampString);
    NanoTime nanoTime = getNanoTime(timestamp, false);
    ByteBuffer buffer = ByteBuffer.wrap(nanoTime.toBinary().getBytes());

    Timestamp result = asTimestamp(fromConstantByteBuffer(buffer));

    assertEquals(timestampString, result.toString());
  }

}
