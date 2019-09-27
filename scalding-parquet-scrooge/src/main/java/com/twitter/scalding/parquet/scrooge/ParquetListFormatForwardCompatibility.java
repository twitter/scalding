package com.twitter.scalding.parquet.scrooge;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

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
    public Type elementType(Type repeatedType) {
      if (repeatedType.isPrimitive()) {
        return repeatedType;
      } else {
        return firstField(repeatedType.asGroupType());
      }
    }

    public Boolean isElementRequired(Type repeatedType) {
      return true;
    }

    public String elementName(Type repeatedType) {
      return this.elementType(repeatedType).getName();
    }

    public OriginalType elementOriginalType(Type repeatedType) {
      return this.elementType(repeatedType).getOriginalType();
    }

    abstract Boolean check(Type type);

    abstract Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType);

  }

  static class RulePrimitiveElement extends Rule {

    public String constantElementName() {
      return "element";
    }

    @Override
    public Boolean check(Type repeatedType) {
      return repeatedType.isPrimitive() && repeatedType.getName().equals(this.constantElementName());
    }

    @Override
    public Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType) {
      if (!isElementRequired) {
        throw new IllegalArgumentException("Rule 1 can only take required element");
      }
      if (!type.isPrimitive()) {
        throw new IllegalArgumentException(
            String.format("Rule 1 cannot take primitive type, but is given %s", type));
      }
      return new PrimitiveType(
          Type.Repetition.REPEATED,
          type.asPrimitiveType().getPrimitiveTypeName(),
          this.constantElementName(),
          originalType
      );
    }

  }

  static class RulePrimitiveArray extends RulePrimitiveElement {
    @Override
    public String constantElementName() {
      return "array";
    }
  }

  static class RuleGroupElement extends Rule {
    public String constantElementName() {
      return "element";
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
    public Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType) {
      if (type.isPrimitive()) {
        return new GroupType(
            Type.Repetition.REPEATED,
            this.constantElementName(),
            type
        );
      } else {
        return new GroupType(
            Type.Repetition.REPEATED,
            this.constantElementName(),
            type.asGroupType().getFields()
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
    public Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType) {
      if (!type.isPrimitive()) {
        throw new IllegalArgumentException(String.format(
            "Rule 3 can only take group type, but found %s", type));
      }
      if (!name.endsWith("_tuple")) {
        name = name + "_tuple";
      }
      return new PrimitiveType(
          Type.Repetition.REPEATED,
          type.asPrimitiveType().getPrimitiveTypeName(),
          name,
          originalType
      );
    }
  }

  static class RuleStandardThreeLevel extends Rule {
    @Override
    public Boolean check(Type repeatedField) {
      if (repeatedField.isPrimitive() || !repeatedField.getName().equals("list")) {
        return false;
      }
      Type elementType = firstField(repeatedField.asGroupType());
      return elementType.getName().equals("element");
    }

    @Override
    public String elementName(Type repeatedType) {
      return "element";
    }

    @Override
    public Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType) {
      Type elementType = null;
      if (type.isPrimitive()) {
        elementType = new PrimitiveType(
            isElementRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL,
            type.asPrimitiveType().getPrimitiveTypeName(),
            "element",
            originalType
        );
      } else {
        elementType = new GroupType(
            isElementRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL,
            "element",
            // we cannot flatten `list`
            type.asGroupType().getName().equals("list") ?
                Arrays.asList(type) :
                type.asGroupType().getFields()
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
      // Recurse on the repeated content. This is to handle nested list
      Type repeatedResolved = resolveTypeFormat(repeatedFile, repeatedProjection);
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
        if (!projected.isPrimitive()) {
          // The file type field must be a group type too
          int fieldIndex = groupFile.getFieldIndex(projected.getName());
          Type fileField = groupFile.getFields().get(fieldIndex);
          fields.add(resolveTypeFormat(fileField.asGroupType(), projected.asGroupType()));
        } else {
          fields.add(projected);
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

  private static GroupUnwrapped unwrapGroup(Type type, Stack<GroupType> wrappers) {
    Type ptr = type;
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
