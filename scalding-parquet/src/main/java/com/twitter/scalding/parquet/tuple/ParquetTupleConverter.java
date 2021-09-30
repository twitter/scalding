package com.twitter.scalding.parquet.tuple;

import cascading.tuple.Tuple;

import com.twitter.scalding.parquet.convert.DecimalUtils;
import com.twitter.scalding.parquet.convert.ParquetTimestampUtils;
import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

public class ParquetTupleConverter extends GroupConverter {

  protected Tuple currentTuple;
  private final Converter[] converters;

  public ParquetTupleConverter(GroupType parquetSchema) {
    int schemaSize = parquetSchema.getFieldCount();

    this.converters = new Converter[schemaSize];
    for (int i = 0; i < schemaSize; i++) {
      Type type = parquetSchema.getType(i);
      converters[i] = newConverter(type, i);
    }
  }

  private Converter newConverter(Type type, int i) {
    if (!type.isPrimitive()) {
      throw new IllegalArgumentException("cascading can only build tuples from primitive types");
    } else {
      Converter converter = null;
      if (TupleDecimalConverter.accepts(type)) {
        converter = new TupleDecimalConverter(type, this, i);
      } else if (TupleInt96DateConverter.accepts(type)) {
        converter = new TupleInt96DateConverter(this, i);
      } else {
        converter = new TuplePrimitiveConverter(this, i);
      }
      return converter;
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  final public void start() {
    currentTuple = Tuple.size(converters.length);
  }

  @Override
  public void end() {
  }

  final public Tuple getCurrentTuple() {
    return currentTuple;
  }

  static final class TuplePrimitiveConverter extends PrimitiveConverter {

    private final ParquetTupleConverter parent;
    private final int index;

    public TuplePrimitiveConverter(ParquetTupleConverter parent, int index) {
      this.parent = parent;
      this.index = index;
    }

    @Override
    public void addBinary(Binary value) {
      parent.getCurrentTuple().setString(index, value.toStringUsingUTF8());
    }

    @Override
    public void addBoolean(boolean value) {
      parent.getCurrentTuple().setBoolean(index, value);
    }

    @Override
    public void addDouble(double value) {
      parent.getCurrentTuple().setDouble(index, value);
    }

    @Override
    public void addFloat(float value) {
      parent.getCurrentTuple().setFloat(index, value);
    }

    @Override
    public void addInt(int value) {
      parent.getCurrentTuple().setInteger(index, value);
    }

    @Override
    public void addLong(long value) {
      parent.getCurrentTuple().setLong(index, value);
    }
  }

  static abstract class BaseTupleConverter extends PrimitiveConverter {

    protected final ParquetTupleConverter parent;
    protected final int index;

    public BaseTupleConverter(ParquetTupleConverter parent, int index) {
      this.parent = parent;
      this.index = index;
    }

    abstract public void addBinary(Binary value);

    @Override
    public void addBoolean(boolean value) {
      parent.getCurrentTuple().setBoolean(index, value);
    }

    @Override
    public void addDouble(double value) {
      parent.getCurrentTuple().setDouble(index, value);
    }

    @Override
    public void addFloat(float value) {
      parent.getCurrentTuple().setFloat(index, value);
    }

    @Override
    public void addInt(int value) {
      parent.getCurrentTuple().setInteger(index, value);
    }

    @Override
    public void addLong(long value) {
      parent.getCurrentTuple().setLong(index, value);
    }
  }

  static final class TupleDecimalConverter extends BaseTupleConverter {

    private static final int MAX_BIG_DECIMAL_PRECISION = 18;
    private static final int MAX_INT_PRECISION = 10;

    private final Type parquetType;

    static boolean accepts(Type type) {
      PrimitiveTypeName ptn = type.asPrimitiveType().getPrimitiveTypeName();
      return (type.isPrimitive() && ptn == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    }

    public TupleDecimalConverter(Type parquetType, ParquetTupleConverter parent, int index) {
      super(parent, index);
      this.parquetType = parquetType;
    }

    @Override
    public void addBinary(Binary value) {
      PrimitiveTypeName ptn = parquetType.asPrimitiveType().getPrimitiveTypeName();
      if (ptn == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        DecimalMetadata dm = parquetType.asPrimitiveType().getDecimalMetadata();
        int precision = dm.getPrecision();
        int scale = dm.getScale();
        BigDecimal bigDecimal = DecimalUtils.binaryToDecimal(value, precision, scale);
        if (scale != 0 || precision > MAX_BIG_DECIMAL_PRECISION) {
          parent.getCurrentTuple().setString(index, bigDecimal.toPlainString());
        } else if (precision <= MAX_INT_PRECISION) {
          parent.getCurrentTuple().setInteger(index, bigDecimal.intValue());
        } else {
          parent.getCurrentTuple().setLong(index, bigDecimal.longValue());
        }
      } else {
        parent.getCurrentTuple().setString(index, value.toStringUsingUTF8());
      }
    }
  }

  static final class TupleInt96DateConverter extends BaseTupleConverter {

    static boolean accepts(Type type) {
      PrimitiveTypeName ptn = type.asPrimitiveType().getPrimitiveTypeName();
      return (type.isPrimitive() && ptn == PrimitiveTypeName.INT96);
    }

    public TupleInt96DateConverter(ParquetTupleConverter parent, int index) {
      super(parent, index);
    }

    @Override
    public void addBinary(Binary value) {
      Timestamp ts = ParquetTimestampUtils.asTimestamp(value);
      parent.getCurrentTuple().setString(index, ts.toString());
    }
  }
}
