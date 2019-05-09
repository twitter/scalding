package com.twitter.scalding.parquet.tuple;

import static org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils.getNanoTime;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import cascading.tuple.Tuple;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParquetTupleConverterTest {

  private static GroupType parquetSchema;

  private static ParquetTupleConverter converter;
  private static PrimitiveConverter tupleConverter;

  @BeforeClass
  public static void setUpClass() {
    String parquetSchemaString = "message spark_schema {\n"
        + "  optional binary basicString (UTF8);\n"
        + "  optional int32 int64field;\n"
        + "  optional int64 int64field;\n"
        + "  optional fixed_len_byte_array(8) decimal180 (DECIMAL(18,0));\n"
        + "  optional fixed_len_byte_array(2) decimal40 (DECIMAL(4,0));\n"
        + "  optional fixed_len_byte_array(8) decimal100 (DECIMAL(10,0));\n"
        + "  optional fixed_len_byte_array(8) decimal120 (DECIMAL(12,0));\n"
        + "  optional fixed_len_byte_array(8) decimal82 (DECIMAL(8,2));\n"
        + "  optional fixed_len_byte_array(8) decimal122 (DECIMAL(12,2));\n"
        + "  optional fixed_len_byte_array(10) decimal190 (DECIMAL(19,0));\n"
        + "  optional fixed_len_byte_array(10) decimal191 (DECIMAL(19,1));\n"
        + "  optional int32 int32date (DATE);\n"
        + "  optional int96 int96date;\n"
        + "}";

    parquetSchema = MessageTypeParser.parseMessageType(parquetSchemaString);
  }

  @Before
  public void setUp() {
    converter = new ParquetTupleConverter(parquetSchema);
  }

  @Test
  public void testConverterCreation() {
    assertThat(converter.getConverter(0),
        is(instanceOf(ParquetTupleConverter.TuplePrimitiveConverter.class)));
    assertThat(converter.getConverter(1),
        is(instanceOf(ParquetTupleConverter.TuplePrimitiveConverter.class)));
    assertThat(converter.getConverter(2),
        is(instanceOf(ParquetTupleConverter.TuplePrimitiveConverter.class)));
    assertThat(converter.getConverter(3),
        is(instanceOf(ParquetTupleConverter.TupleDecimalConverter.class)));
    assertThat(converter.getConverter(4),
        is(instanceOf(ParquetTupleConverter.TupleDecimalConverter.class)));
    assertThat(converter.getConverter(5),
        is(instanceOf(ParquetTupleConverter.TupleDecimalConverter.class)));
    assertThat(converter.getConverter(6),
        is(instanceOf(ParquetTupleConverter.TupleDecimalConverter.class)));
    assertThat(converter.getConverter(7),
        is(instanceOf(ParquetTupleConverter.TupleDecimalConverter.class)));
    assertThat(converter.getConverter(8),
        is(instanceOf(ParquetTupleConverter.TupleDecimalConverter.class)));
    assertThat(converter.getConverter(9),
        is(instanceOf(ParquetTupleConverter.TupleDecimalConverter.class)));
    assertThat(converter.getConverter(10),
        is(instanceOf(ParquetTupleConverter.TupleDecimalConverter.class)));
    assertThat(converter.getConverter(11),
        is(instanceOf(ParquetTupleConverter.TuplePrimitiveConverter.class)));
    assertThat(converter.getConverter(12),
        is(instanceOf(ParquetTupleConverter.TupleInt96DateConverter.class)));
  }

  @Test
  public void testConvertDecimal180() {
    int index = 3;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.asLongBuffer().put(12345);
    Binary value = Binary.fromConstantByteBuffer(buffer);

    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getLong(index), is(12345L));
  }

  @Test
  public void testConvertDecimal40() {
    int index = 4;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));

    ByteBuffer buffer = ByteBuffer.wrap(intToTwoByteArray(123));
    Binary value = Binary.fromConstantByteBuffer(buffer);

    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getInteger(index), is(123));
  }

  @Test
  public void testConvertDecimal100() {
    int index = 5;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.asLongBuffer().put(1234567890);
    Binary value = Binary.fromConstantByteBuffer(buffer);

    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getInteger(index), is(1234567890));
  }

  @Test
  public void testConvertDecimal120() {
    int index = 6;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.asLongBuffer().put(123456789012L);
    Binary value = Binary.fromConstantByteBuffer(buffer);

    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getLong(index), is(123456789012L));
  }

  @Test
  public void testConvertDecimal82() {
    int index = 7;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.asLongBuffer().put(1234567);
    Binary value = Binary.fromConstantByteBuffer(buffer);

    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getString(index), is("12345.67"));
  }

  @Test
  public void testConvertDecimal122() {
    int index = 8;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.asLongBuffer().put(1234567891);
    Binary value = Binary.fromConstantByteBuffer(buffer);

    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getString(index), is("12345678.91"));
  }

  @Test
  public void testConvertDecimal190() {
    int index = 9;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));
    Binary value = Binary
        .fromConstantByteArray(new BigInteger("1234567890123456789").toByteArray());
    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getString(index), is("1234567890123456789"));
  }

  @Test
  public void testConvertDecimal191() {
    int index = 10;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));
    Binary value = Binary
        .fromConstantByteArray(new BigInteger("1234567890123456789").toByteArray());

    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getString(index), is("123456789012345678.9"));
  }

  @Test
  public void testConvertInt96Date() {
    int index = 12;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));
    Binary value = timestampStringToBinary("2011-01-01 00:00:00.123100001");

    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getString(index), is("2011-01-01 00:00:00.123100001"));
  }

  @Test
  public void testConvertInt96DateAnoherDate() {
    int index = 12;
    tupleConverter = (PrimitiveConverter) converter.getConverter(index);

    assertThat(tupleConverter, is(notNullValue()));
    Binary value = timestampStringToBinary("2015-12-17 16:19:59.192837465");

    converter.start();
    tupleConverter.addBinary(value);
    converter.end();

    Tuple tuple = converter.getCurrentTuple();

    assertThat(tuple.getString(index), is("2015-12-17 16:19:59.192837465"));
  }

  public static final byte[] intToTwoByteArray(int value) {
    return new byte[]{
        (byte) (value >>> 8),
        (byte) value};
  }

  public static Binary timestampStringToBinary(String timestampString) {
    Timestamp timestamp = Timestamp.valueOf(timestampString);
    NanoTime nanoTime = getNanoTime(timestamp, false);
    return nanoTime.toBinary();
  }
}
