package com.twitter.scalding.parquet.scrooge;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.thrift.DecodingSchemaMismatchException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * Compatibility class to convert parquet schema of legacy type to standard one
 * namely 3-level list structure as recommended in
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
 *
 * More specifically this handles converting from parquet file created by
 * {{@code org.apache.parquet.thrift.ThriftSchemaConvertVisitor}} which always suffix
 * list element with "_tuple".
 */
public class ParquetListFormatForwardCompatibility {

  private static List<Rule> RULES = Arrays.asList(
      new RulePrimitiveElement(),
      new RulePrimitiveArray(),

      new RuleGroupElement(),
      new RuleGroupArray(),

      new RuleGroupTuple(),
      new RuleStandardThreeLevel());

  /**
   * Rule describes how to match a repeated type, how to decompose them, and reconstruct a
   * repeated type.
   */
  abstract static public class Rule {
    abstract public Type elementType(Type repeatedType);

    abstract Boolean isElementRequired(Type repeatedType);

    public String elementName(Type repeatedType) {
      return this.elementType(repeatedType).getName();
    }

    public OriginalType elementOriginalType(Type repeatedType) {
      return this.elementType(repeatedType).getOriginalType();
    }

    abstract Boolean check(Type typ);

    abstract Type createCompliantRepeatedType(Type typ, String name, Boolean isElementRequired, OriginalType originalType);

  }

  static class RulePrimitiveElement extends Rule {
    /**
     * repeated int32 element;
     */

    public String constantElementName() {
      return "element";
    }

    public Type elementType(Type repeatedType) {
      return repeatedType;
    }

    @Override
    Boolean isElementRequired(Type repeatedType) {
      // According to Rule 1 from,
      // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
      // "the repeated field is not a group,
      // then its type is the element type and elements are required."
      return true;
    }

    @Override
    public Boolean check(Type repeatedType) {
      return repeatedType.isPrimitive() && repeatedType.getName().equals(this.constantElementName());
    }

    @Override
    public Type createCompliantRepeatedType(Type typ, String name, Boolean isElementRequired, OriginalType originalType) {
      if (!isElementRequired) {
        throw new IllegalArgumentException("Rule 1 can only take required element");
      }
      if (!typ.isPrimitive()) {
        throw new IllegalArgumentException(
            String.format("Rule 1 cannot take primitive type, but is given %s", typ));
      }
      return new PrimitiveType(
          Type.Repetition.REPEATED,
          typ.asPrimitiveType().getPrimitiveTypeName(),
          this.constantElementName(),
          originalType
      );
    }

  }

  static class RulePrimitiveArray extends RulePrimitiveElement {
    /**
     * repeated binary array (UTF8);
     */

    @Override
    public String constantElementName() {
      return "array";
    }
  }

  static class RuleGroupElement extends Rule {
    /**
     * repeated group element {
     *   required binary str (UTF8);
     *   required int32 num;
     * };
     */
    public String constantElementName() {
      return "element";
    }

    public Boolean isElementRequired(Type repeatedType) {
      // According Rule 2 from
      // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
      // "If the repeated field is a group with multiple fields,
      // then its type is the element type and elements are required."
      return true;
    }

    public Type elementType(Type repeatedType) {
      return repeatedType;
    }

    @Override
    public String elementName(Type repeatedType) {
      return this.constantElementName();
    }

    @Override
    public Boolean check(Type repeatedType) {
      if (repeatedType.isPrimitive()) {
        return false;
      } else {
        GroupType repeatedGroup = repeatedType.asGroupType();
        return repeatedGroup.getFields().size() > 0 && repeatedGroup.getName().equals(this.constantElementName());
      }
    }

    @Override
    public Type createCompliantRepeatedType(Type typ, String name, Boolean isElementRequired, OriginalType originalType) {
      if (typ.isPrimitive()) {
        return new GroupType(
            Type.Repetition.REPEATED,
            this.constantElementName(),
            typ
        );
      } else {
        return new GroupType(
            Type.Repetition.REPEATED,
            this.constantElementName(),
            typ.asGroupType().getFields()
        );
      }
    }
  }

  static class RuleGroupArray extends RuleGroupElement {
    @Override
    public String constantElementName() {
      return "array";
    }
  }

  static class RuleGroupTuple extends Rule {

    @Override
    public Boolean check(Type repeatedType) {
      return repeatedType.getName().endsWith("_tuple");
    }

    public Type elementType(Type repeatedType) {
      return repeatedType;
    }

    @Override
    Boolean isElementRequired(Type repeatedType) {
      // According to Rule 3 from
      // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
      return true;
    }

    @Override
    public Type createCompliantRepeatedType(Type typ, String name, Boolean isElementRequired, OriginalType originalType) {
      if (!name.endsWith("_tuple")) {
        name = name + "_tuple";
      }
      if (typ.isPrimitive()) {
        return new PrimitiveType(
            Type.Repetition.REPEATED,
            typ.asPrimitiveType().getPrimitiveTypeName(),
            name,
            originalType
        );
      } else {
        return new GroupType(
            Type.Repetition.REPEATED,
            name,
            OriginalType.LIST,
            typ.asGroupType().getFields()
        );
      }
    }
  }

  static class RuleStandardThreeLevel extends Rule {
    /**
     * <list-repetition> group <name> (LIST) {
     *   repeated group list {
     *     <element-repetition> <element-type> element;
     *   }
     * }
     */

    @Override
    public Boolean check(Type repeatedField) {
      if (repeatedField.isPrimitive() || !repeatedField.getName().equals("list")) {
        return false;
      }
      Type elementType = firstField(repeatedField.asGroupType());
      return elementType.getName().equals("element");
    }

    @Override
    public Type elementType(Type repeatedType) {
      return firstField(repeatedType.asGroupType());
    }

    @Override
    Boolean isElementRequired(Type repeatedType) {
      return elementType(repeatedType).getRepetition() == Type.Repetition.REQUIRED;
    }

    @Override
    public String elementName(Type repeatedType) {
      return "element";
    }

    @Override
    public Type createCompliantRepeatedType(Type typ, String name, Boolean isElementRequired, OriginalType originalType) {

      Type elementType;
      if (typ.isPrimitive()) {
        elementType = new PrimitiveType(
            isElementRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL,
            typ.asPrimitiveType().getPrimitiveTypeName(),
            "element",
            originalType
        );
      } else {
        elementType = new GroupType(
            isElementRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL,
            "element",
            isGroupList(typ) ? OriginalType.LIST: null,
            // we cannot flatten `list`
            typ.asGroupType().getName().equals("list") ?
                Arrays.asList(typ) :
                typ.asGroupType().getFields()
        );
      }
      return new GroupType(
          Type.Repetition.REPEATED,
          "list",
          Arrays.asList(elementType)
      );
    }
  }

  private static org.apache.parquet.schema.Type firstField(GroupType groupType) {
    return groupType.getFields().get(0);
  }

  private static boolean isGroupList(Type projection) {
    if (projection.isPrimitive()) {
      return false;
    }
    GroupType groupProjection = projection.asGroupType();
    return groupProjection.getOriginalType() == OriginalType.LIST &&
        groupProjection.getFieldCount() == 1 &&
        groupProjection.getFields().get(0).isRepetition(Type.Repetition.REPEATED);
  }

  private static boolean isGroupMap(Type projection) {
    if (projection.isPrimitive()) {
      return false;
    }
    GroupType groupProjection = projection.asGroupType();
    return groupProjection.getOriginalType() == OriginalType.MAP &&
        groupProjection.getFieldCount() == 1 &&
        groupProjection.getFields().get(0).isRepetition(Type.Repetition.REPEATED) &&
        (
            (groupProjection.getFields().get(0).getName().equals("map") &&
                groupProjection.getFields().get(0).getOriginalType() == OriginalType.MAP_KEY_VALUE)
            || groupProjection.getFields().get(0).getName().equals("key_value")
        );
  }

  public Type elementType(Type repeatedType, String debuggingTypeSource) {
    Rule fileTypeRule = findFirstRule(repeatedType, debuggingTypeSource);
    return fileTypeRule.elementType(repeatedType);
  }

  public Type wrap(Type repeatedType, Type elementType) {
    Rule projectedTypeRule = findFirstRule(repeatedType, "projection");

    return projectedTypeRule.createCompliantRepeatedType(
        elementType,
        elementType.getName(),
        // if repeated or required, it is required
        !elementType.isRepetition(Type.Repetition.OPTIONAL),
        elementType.getOriginalType());
  }

  /**
   * Resolve list format in forward compatible way.
   * @param fileType   file type which has new format
   * @param projection projection type which has legacy format
   * @return projection schema in the new format.
   */
  public Type resolveTypeFormat(Type fileType, Type projection) {
    if (projection.isPrimitive() || fileType.isPrimitive()) {
      return projection;
    }
    ParquetListFormatForwardCompatibility compatibility = new ParquetListFormatForwardCompatibility();

    GroupType groupFile = fileType.asGroupType();
    GroupType groupProjection = projection.asGroupType();

    GroupUnwrapped unwrappedFile = unwrapGroup(groupFile, new Stack<GroupType>());
    GroupUnwrapped unwrappedProjection = unwrapGroup(groupProjection, new Stack<GroupType>());

    Type repeatedFile = unwrappedFile.repeatedType;
    Type repeatedProjection = unwrappedProjection.repeatedType;

    if (repeatedProjection != null && repeatedFile != null) {
      // Repeated types cannot be recursed yet, because file and projection might have
      // format-specific wrappers. Instead, we need to extract its element type first.
      // Eg. without unwrapping `repeated` layer, we will only find `element` field in the file type
      // File type:                             |     Projection type:
      // optional group foo (LIST) {            |     optional group foo (LIST) {
      //   repeated group list {                |       repeated group foo_tuple {
      //     required group element {           |         optional binary zing (UTF8);
      //       required binary zing (UTF8);     |         optional binary bar (UTF8);
      //       required binary bar (UTF8);      |       }
      //     }                                  |     }
      //   }
      // }
      Type elementFile = compatibility.elementType(repeatedFile, "file");
      Type elementProjection = compatibility.elementType(repeatedProjection, "projection");

      // Recurse on the element. This is to handle nested list
      Type elementResolved = resolveTypeFormat(elementFile, elementProjection);
      // Wrap
      Type repeatedResolved = compatibility.wrap(repeatedProjection, elementResolved);

      // Make projected structure compatible with file type
      Type repeatedFormatted = compatibility
          .makeForwardCompatible(repeatedFile, repeatedResolved);

      // Wrap back the groups, this contain field name and whether it's optional/required
      Type resolvedGroupType = repeatedFormatted;
      while (!unwrappedProjection.wrappers.isEmpty()) {
        resolvedGroupType = unwrappedProjection.wrappers.pop().withNewFields(resolvedGroupType);
      }
      return resolvedGroupType;
    } else {
      List<Type> fields = new ArrayList<Type>();
      for (Type projected : groupProjection.getFields()) {
        if (!groupFile.containsField(projected.getName())) {
          // This can happen when
          // 1) projecting optional field over non-existent target schema
          // 2) field is a part of legacy map format
          // Make no assertions (separation of responsibility) and just include it
          fields.add(projected);
        } else {
          int fieldIndex = groupFile.getFieldIndex(projected.getName());
          Type fileField = groupFile.getFields().get(fieldIndex);
          fields.add(resolveTypeFormat(fileField, projected));
        }
      }
      return groupProjection.withNewFields(fields);
    }
  }

  private Rule findFirstRule(Type repeatedType, String debuggingTypeSource) {
    Rule matchedRule = null;
    for (Rule rule : RULES) {
      if (rule.check(repeatedType)) {
        matchedRule = rule;
        break;
      }
    }
    if (matchedRule == null) {
      throw new RuntimeException(String.format(
          "Unable to find matching rule for %s schema:\n%s", debuggingTypeSource, repeatedType));
    }
    return matchedRule;
  }

  private Type makeForwardCompatible(Type repeatedFileType, Type repeatedProjectedType) {
    Rule fileTypeRule = findFirstRule(repeatedFileType, "file");
    Rule projectedTypeRule = findFirstRule(repeatedProjectedType, "projected");

    if (projectedTypeRule == fileTypeRule) {
      return repeatedProjectedType;
    }

    String elementName = projectedTypeRule.elementName(repeatedProjectedType);
    Type elementType = projectedTypeRule.elementType(repeatedProjectedType);
    Boolean isElementRequired = projectedTypeRule.isElementRequired(repeatedProjectedType);
    OriginalType elementOriginalType = projectedTypeRule.elementOriginalType(repeatedProjectedType);

    return fileTypeRule.createCompliantRepeatedType(
        elementType,
        elementName,
        isElementRequired,
        elementOriginalType);
  }

  private static class GroupUnwrapped {
    Stack<GroupType> wrappers;
    Type repeatedType;

    public GroupUnwrapped(Stack<GroupType> wrappers, Type repeatedType) {
      this.wrappers = wrappers;
      this.repeatedType = repeatedType;
    }
  }

  private static GroupUnwrapped unwrapGroup(Type typ, Stack<GroupType> wrappers) {
    Type ptr = typ;
    // only wrapper for list with size one, so we can wrap repeated type later
    while (!ptr.isPrimitive()) {
      wrappers.push(ptr.asGroupType());
      if (isGroupList(ptr)) {
        // when it is repeated
        return new GroupUnwrapped(wrappers, ptr.asGroupType().getFields().get(0));
      } else if (ptr.asGroupType().getFields().size() == 1){
        ptr = ptr.asGroupType().getFields().get(0);
      } else {
        break;
      }
    }
    return new GroupUnwrapped(wrappers, null);
  }
}
