package com.twitter.scalding.commons.thrift;

import java.io.ByteArrayInputStream;
import java.util.*;

import org.apache.thrift.TBaseHelper;
import org.apache.thrift.TException;

import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;

import org.apache.thrift.transport.TIOStreamTransport;


/**
 * Allows Thrift objects to be compared in a streaming fashion,
 * reading a field at a time. This can lead to significant savings
 * when sorting serialized Thrift objects -- up to 7x according to
 * our benchmarks.
 *
 * One would assume that Thrift objects are serialized
 * in field order. Alas, Scrooge don't play that way for Scala
 * (java codegen does).
 *
 * Maps and Sets in Thrift do not have a guaranteed sort order when
 * serialized, which means byte arrays cannot be directly compared.
 * We compare all other fields in a streaming fashion, but switch to
 * deserializing the whole Map or Set when we encounter one. Once the
 * maps are compared, we drop back into streaming.
 *
 * <b>Important:</b> one should not rely on the results of this sort
 * being identical to the results of sort based on sorting deserialized
 * objects. In at least 1 case (unions) they will not be identical.
 * There are notes in the source code here explaining why that is -- sadly,
 * unavoidable.
 *
 *
 */
public final class ThriftStreamCompare  {

  static final int GREATER = 1;
  static final int LESS = -1;
  static final int EQUAL = 0;

  private static class CompareState {

    private int decisionThreshold;
    private int terminalFieldIdx;
    private int compValue;


    public CompareState(int decisionThreshold, int terminalFieldIdx, int compValue) {
      this.decisionThreshold = decisionThreshold;
      this.terminalFieldIdx = terminalFieldIdx;
      this.compValue = compValue;
    }

    public int getCompValue() {
      return compValue;
    }

    public void setCompValue(int compValue) {
      this.compValue = compValue;
    }

    public int getTerminalFieldIdx() {
      return terminalFieldIdx;
    }

    public void setTerminalFieldIdx(int terminalFieldIdx) {
      this.terminalFieldIdx = terminalFieldIdx;
    }

    public int getDecisionThreshold() {
      return decisionThreshold;
    }

    public void setDecisionThreshold(int decisionThreshold) {
      this.decisionThreshold = decisionThreshold;
    }
  }

  private ThriftStreamCompare() {
    // Utility classes should not have a public or default constructor.
  }
  /**
   * Used to hold structs for comparison purposes when we are inside maps or sets.
   */
  protected static class ComparableTuple
      implements Comparable<ComparableTuple> {

    private List<Object> data = new ArrayList<Object>();

    public void add(Object o) {
      data.add(o);
    }

    @Override
    public int compareTo(ComparableTuple o) {
      if (o == null) {
        return GREATER;
      }

      int compvalue = EQUAL;
      for (int i = 0; i < data.size(); i++) {
        compvalue = TBaseHelper.compareTo(data.get(i),
            (i < o.data.size()) ? o.data.get(i) : null);
        if (compvalue != EQUAL) {
          if (i % 3 == 0) {
            // the logic is reversed for field indexes.
            // smalled field id => bigger object.
            compvalue = -1 * compvalue;
          }
          break;
        }
      }
      return compvalue;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {

      return o.getClass() == this.getClass() && this.compareTo((ComparableTuple) o) == 0;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("CT[");
      for (Object o : data) {
        sb.append(o).append(", ");
      }
      sb.replace(sb.length() - 2, sb.length(), "]");
      return sb.toString();
    }
  }

  protected static TIOStreamTransport getTTransport(byte[] buf, int start, int length) {
    ByteArrayInputStream is = new ByteArrayInputStream(buf, start, length);
    TIOStreamTransport transport = new TIOStreamTransport(is);
    return transport;
  }

  protected static Object readField(byte type, TProtocol reader) throws TException {
    switch (type) {
      case TType.BOOL:
        return reader.readBool();
      case TType.BYTE:
        return reader.readByte();
      case TType.DOUBLE:
        return reader.readDouble();
      case TType.ENUM:
        return reader.readI32();
      case TType.I16:
        return reader.readI16();
      case TType.I32:
        return reader.readI32();
      case TType.I64:
        return reader.readI64();
      case TType.STRING:
        // Thrift represents both the binary and the string type as TType.STRING
        // and it's only genned code that treats them differently. Safest to just
        // compare binary data, not do the string conversion as the data may not
        // by UTF8. Cheaper, too.
        // UTF8 Strings can be safely compared in binary form.
        // http://en.wikipedia.org/wiki/UTF-8#General states:
        // "Sorting a set of UTF-8 encoded strings as strings of unsigned bytes
        // yields the same order as sorting the corresponding Unicode strings
        // lexicographically by codepoint."
        return reader.readBinary();
      case TType.MAP:
        return readMap(reader);
      case TType.LIST:
        return readList(reader);
      case TType.STRUCT:
        return readStruct(reader);
      case TType.SET:
        return readSet(reader);
      default:
        // WTF
        throw new RuntimeException("unrecognized TType: " + type);
    }
  }

  protected static ComparableTuple readStruct(TProtocol reader) throws TException {
    reader.readStructBegin();
    ComparableTuple struct = new ComparableTuple();
    while (true) {
      TField field = reader.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      // we add the field id and type to the struct so that they are compared
      // otherwise we risk a bug: optional field 1 in item 1 = "foo",
      // optional field 1 in item 2 is absent, and field 2 in item 2 = "foo"
      // we don't want these to look equal.
      struct.add(field.id);
      struct.add(field.type);
      struct.add(readField(field.type, reader));
      reader.readFieldEnd();
    }
    reader.readStructEnd();
    return struct;
  }

  protected static List readList(TProtocol reader) throws TException {
    TList tlistInfo = reader.readListBegin();
    List list = new ArrayList(tlistInfo.size);
    for (int i = 0; i < tlistInfo.size; i++) {
      list.add(readField(tlistInfo.elemType, reader));
    }
    reader.readListEnd();
    return list;
  }

  protected static Map readMap(TProtocol reader) throws TException {
    TMap tmapInfo = reader.readMapBegin();
    Map map = new HashMap(tmapInfo.size);
    for (int i = 0; i < tmapInfo.size; i++) {
      Object key = null;
      Object val = null;
      key = readField(tmapInfo.keyType, reader);
      val = readField(tmapInfo.valueType, reader);
      map.put(key, val);
    }
    reader.readMapEnd();
    return map;
  }

  protected static Set readSet(TProtocol reader) throws TException {
    TSet tsetInfo = reader.readSetBegin();
    Set set = new HashSet(tsetInfo.size);
    for (int i = 0; i < tsetInfo.size; i++) {
      set.add(readField(tsetInfo.elemType, reader));
    }
    reader.readSetEnd();
    return set;
  }

  protected static int listCompare(TProtocol reader1, TProtocol reader2) throws TException {
    TList list1 = reader1.readListBegin();
    TList list2 = reader2.readListBegin();
    // if list types don't match, one with "bigger" type wins
    // if lists sizes don't match, longer list wins
    // otherwise compare elements.
    int compvalue = TBaseHelper.compareTo(list1.elemType, list2.elemType);
    if (compvalue != EQUAL) {
      return compvalue;
    }

      compvalue = TBaseHelper.compareTo(list1.size, list2.size);
    if (compvalue != EQUAL) {
      return compvalue;
    }

    for (int i = 0; i < list1.size; i++) {
      if (compvalue != EQUAL) {
        break;
      } else {
        Object value1 = readField(list1.elemType, reader1);
        Object value2 = readField(list1.elemType, reader2);
        compvalue = TBaseHelper.compareTo(value1, value2);
      }
    }

    reader1.readListEnd();
    reader2.readListEnd();
    return compvalue;
  }

  // Worth noting: based on my reading of the code in TUnion.java,
  // a union is a struct with a single field set. When comparing
  // unions, unfortunately Thrift generates code that does the opposite
  // order than what structs with optional fields compare as.
  // We go with the logic that an object having a set field with an earlier
  // id wins. This is what TBase does for structs with optional fields.
  //
  // SO THIS WILL RESULT IN A CONSISTENT ORDER, BUT FOR UNIONS IT IS
  // DIFFERENT THAN OBJECT ORDER
  protected static int findWinner(BitSet setFields1, BitSet setFields2,
                                int decisionThreshold,
                                int compvalue) {
    // if there are any fields that are set in 1 object but not in another
    // with ids below decisionThreshold, winner is decided based on lowest
    // id of such a field. Otherwise, the winner is based on compvalue.

    // eh, I'm sure this can be made much cleaner with clever xors.
    setFields1.clear(Math.min(setFields1.length(), decisionThreshold), setFields1.length());
    setFields2.clear(Math.min(setFields2.length(), decisionThreshold), setFields2.length());

    if (!setFields1.isEmpty() || !setFields2.isEmpty()) {
      // if any fields before threshold are set, find ones
      // that are set in one and not the other
      BitSet oneNotTwo = (BitSet) setFields1.clone();
      oneNotTwo.andNot(setFields2);

      BitSet twoNotOne = (BitSet) setFields2.clone();
      twoNotOne.andNot(setFields1);

      // if such fields exist, winner is one with lowest such field idx
      if (!oneNotTwo.isEmpty() || !twoNotOne.isEmpty()) {
        if (oneNotTwo.isEmpty()) {
          return LESS;
        } else if (twoNotOne.isEmpty()) {
          return GREATER;
        } else {
         int earliestSet1 = oneNotTwo.nextSetBit(0);
         int earliestSet2 = twoNotOne.nextSetBit(0);
          // equals is impossible by construction
         return (earliestSet1 > earliestSet2) ? LESS : GREATER;
        }
      }
      // if both are empty, all fields set in 1 were set in 2, so
      // it's the result of value comparison at decision threshold
    }

    // it's result of comparing values at decision threshold.
    return compvalue;
  }

  /**
   * Reads from the reader, checks against existing state,
   * tries to figure out if we have information to make
   * a decision. Puts things in the bitsets and valuemaps.
   * Updates compareState with result of computation.
   * This is the least functional thing ever. Sorry, Oscar.
   *
   * @param reader the thing to read from
   * @param compareState current decision threshold, compvalue, etc
   * @param setFields1 which fields have been seen for object being read
   * @param setFields2 which fields have been seen for object being compared to
   * @param tfield what kind of field we're reading
   * @param valueMap1 values of known fields from object being read
   * @param valueMap2 values of known fields from object being compared to
   * @return true if ultimate decision is reached.
   * @throws TException
   */
  private static boolean readAndCompare(TProtocol reader,
                                    CompareState compareState,
                                    BitSet setFields1, BitSet setFields2,
                                    TField tfield,
                                    Map<Short, Object> valueMap1,
                                    Map<Short, Object> valueMap2,
                                    boolean reverseCompare) throws TException {

    // This is a bit gnarly.
    // If we don't need to read this field to decide, just skip it.
    // Otherwise, read the value. If there is a corresponding value in
    // the other map, compare them. If non-equal, record result of comparison
    // and move decision threshold.
    // if this was the known terminating field, we are done; otherwise,
    // we will keep going.
    if (tfield.id > compareState.getDecisionThreshold()) {
      TProtocolUtil.skip(reader, tfield.type);
    } else {
      valueMap1.put(tfield.id, readField(tfield.type, reader));
      setFields1.set(tfield.id);
      if (valueMap2.containsKey(tfield.id)) {
        int value = TBaseHelper.compareTo(valueMap1.get(tfield.id),
            valueMap2.get(tfield.id));
        if (reverseCompare) {
          value = value * -1;
        }
        if (value != 0) {
          compareState.setDecisionThreshold(tfield.id);
          compareState.setCompValue(value);
          if (tfield.id == compareState.getTerminalFieldIdx()) {
            return true;
          }
        } else if (tfield.id == compareState.getTerminalFieldIdx()) {
          compareState.setTerminalFieldIdx(tfield.id + 1);
        }
        valueMap1.remove(tfield.id);
        valueMap2.remove(tfield.id);
      }
    }
    reader.readFieldEnd();
    return false;
  }

  /**
   * Compares structs represented by primed TProtocols, in a streaming fashion.
   *
   * @param reader1
   * @param reader2
   * @return &lt; 0 if obj 1 &lt; obj2, 0 if equal, &gt; 0 if &gt;
   */
  public static int structCompare(TProtocol reader1, TProtocol reader2,
                                  short minFieldId) {

    /**
     * There is no contract in Thrift that says the data has to be serialized
     * in field id order. In fact, Scrooge does not do this (it serializes
     * in IDL order -- this means not only is it not id order, it might
     * change on you). However, it's reasonable to assume that the vast
     * majority of the cases will have fields in order; that's what happens in
     * practice. So we optimize for that case, and handle the out of order case.
     *
     * Unfortunately this means the logic below is somewhat complex. Here's
     * what's going on:
     *
     *
     * Keep track of smallest field id where we saw a difference, called
     * decisionThreshold (MAX_INT initially).
     *
     * Keep track of decision so far in currentCompResult.
     *
     * Keep a bitset for each object representing which fields have values.
     * (resize the bitset as necessary).
     *
     * When reading a field:
     *   -- for each field we see, set the bit for this field being set
     *   -- if id > decisionThreshold, skip.
     *      otherwise, read field from reader1.
     *      if map2 already has a value, compare (see below).
     *      otherwise, stick value into map1.
     *   -- same deal for field from reader2.
     *
     *
     * comparisons:
     * compvalue = compare the values.
     * toss the data from map1 and map2.
     * we should only get here if fieldId < decisionThreshold but double check
     * just in case before setting decisionThreshold.
     * if not equal, set decisionThreshold to this id.
     * set currentCompResult to result.
     * if equal, we just move on.
     *
     * at the end: check if there are any fields in either object with ids < decision
     * threshold which are set in one object, but not set in another. If so,
     * the one with the lowest field that is set in it, but not in the other, wins.
     * Otherwise, return result of comparison at decisionThreshold.
     *
     * Optimization for cases when things are in order:
     * Keep track of smallest field id which will tip the decision.
     * This is the terminalField.
     * Increment it appropriately if field 1, 2, etc are equal.
     */

    BitSet setFields1 = new BitSet();
    BitSet setFields2 = new BitSet();

    CompareState compareState = new CompareState(Short.MAX_VALUE, minFieldId, EQUAL);

    Map<Short, Object> valueMap1 = new HashMap<Short, Object>();
    Map<Short, Object> valueMap2 = new HashMap<Short, Object>();

    boolean keepReading1 = true;
    boolean keepReading2 = true;

    TField field1 = null;
    TField field2 = null;
    try {
      reader1.readStructBegin();
      reader2.readStructBegin();
      while (true) {
        if (keepReading1) {
          field1 = reader1.readFieldBegin();
        }
        if (keepReading2) {
          field2 = reader2.readFieldBegin();
        }
        // these always happen the first time through
        // so no NPE possible below.
        keepReading1 = field1.type != TType.STOP;
        keepReading2 = field2.type != TType.STOP;

        if (!keepReading1 && !keepReading2) {
          break;
        }
        // TODO: need to do something about different types at same field id.

        // Lists and structs
        // are special cased cause we can terminate early out of them.
        if (field1.id == field2.id
            && field1.type == field2.type
            && (field1.type == TType.LIST
                || field1.type == TType.STRUCT)
            && field1.id <= compareState.getDecisionThreshold()
            && field1.id == compareState.getTerminalFieldIdx()) {
          int value = (field1.type == TType.LIST)
              ? listCompare(reader1, reader2)
              : structCompare(reader1, reader2, minFieldId);
          if (value != EQUAL) {
            compareState.setDecisionThreshold(field1.id);
            compareState.setCompValue(value);
            return findWinner(setFields1, setFields2,
                compareState.getDecisionThreshold(),
                compareState.getCompValue());
          } else {
            reader1.readFieldEnd();
            reader2.readFieldEnd();
            compareState.setTerminalFieldIdx(compareState.getTerminalFieldIdx() + 1);
          }
        } else {

          if (keepReading1) {
            boolean decisionReached = readAndCompare(reader1, compareState,
                setFields1, setFields2, field1,
                valueMap1, valueMap2, false);

            if (decisionReached) {
              return compareState.getCompValue();
            }
          }

          if (keepReading2) {
            boolean decisionReached = readAndCompare(reader2, compareState,
                setFields2, setFields1, field2,
                valueMap2, valueMap1, true);
            if (decisionReached) {
              return compareState.getCompValue();
            }
          }
        }
      }
      reader1.readStructEnd();
      reader2.readStructEnd();
    } catch (TException e) {
      throw new RuntimeException("Error when comparing in raw comparison", e);
    }
    return findWinner(setFields1, setFields2,
        compareState.getDecisionThreshold(),
        compareState.getCompValue());
  }

  /**
   * Compare two serialized Thrift objects in a streaming fashion.
   * @param b1 buffer holding first serialized object
   * @param s1 start offset for this object
   * @param l1 length of the serialized object
   * @param b2 buffer holding second serialized object
   * @param s2 start offset for second object
   * @param l2 length of the second serialzied object
   * @param tFactory factory generating appropriate protocol (TBinary, TCompact, etc).
   * @return &lt; 0 if obj 1 &lt; obj2, 0 if equal, &gt; 0 if &gt;
   */
  public static int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2,
                            short minFieldId, TProtocolFactory tFactory) {
    TIOStreamTransport transport1 = getTTransport(b1, s1, l1);
    TIOStreamTransport transport2 = getTTransport(b2, s2, l2);
    TProtocol reader1 = tFactory.getProtocol(transport1);
    TProtocol reader2 = tFactory.getProtocol(transport2);
    return structCompare(reader1, reader2, minFieldId);
  }
}
