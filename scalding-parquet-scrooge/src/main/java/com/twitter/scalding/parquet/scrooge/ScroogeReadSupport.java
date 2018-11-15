/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.twitter.scalding.parquet.scrooge;

import com.twitter.scrooge.ThriftStruct;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.thrift.ThriftReadSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.apache.parquet.thrift.ThriftMetaData;
import org.apache.parquet.thrift.ThriftSchemaConverter;
import org.apache.parquet.thrift.projection.FieldProjectionFilter;
import org.apache.parquet.thrift.projection.ThriftProjectionException;
import org.apache.parquet.thrift.struct.ThriftType;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Read support for Scrooge
 *
 * @author Tianshuo Deng
 */
public class ScroogeReadSupport<T extends ThriftStruct> extends ThriftReadSupport<T> {

  /**
   * used from hadoop
   * the configuration must contain a "parquet.thrift.read.class" setting
   */
  public ScroogeReadSupport() {
  }

  @Override
  protected MessageType getProjectedSchema(FieldProjectionFilter fieldProjectionFilter) {
    ThriftType.StructType thriftStruct = new ScroogeStructConverter().convert(thriftClass);
    return new ThriftSchemaConverter(fieldProjectionFilter).convert(thriftStruct);
  }

  /**
   * Method copied from ThriftReadSupport
   *
   * @param context
   * @return
   */
  @Override
  public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
    final Configuration configuration = context.getConfiguration();
    final MessageType fileMessageType = context.getFileSchema();
    MessageType requestedProjection = fileMessageType;
    String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);

    FieldProjectionFilter projectionFilter = getFieldProjectionFilter(configuration);

    if (partialSchemaString != null && projectionFilter != null) {
      throw new ThriftProjectionException(
        String.format("You cannot provide both a partial schema and field projection filter."
            + "Only one of (%s, %s, %s) should be set.",
          PARQUET_READ_SCHEMA, STRICT_THRIFT_COLUMN_FILTER_KEY, THRIFT_COLUMN_FILTER_KEY));
    }

    //set requestedProjections only when it's specified
    if (partialSchemaString != null) {
      requestedProjection = getSchemaForRead(fileMessageType, partialSchemaString);
    } else if (projectionFilter != null) {
      try {
        initThriftClassFromMultipleFiles(context.getKeyValueMetadata(), configuration);
        requestedProjection = getProjectedSchema(projectionFilter);
      } catch (ClassNotFoundException e) {
        throw new ThriftProjectionException("can not find thriftClass from configuration", e);
      }
    }

    MessageType schemaForRead = getSchemaForRead(fileMessageType, requestedProjection);
    return new ReadContext(schemaForRead);
  }

  /**
   * Method from ReadSupport, copied because it's static
   *
   * @param fileMessageType
   * @param partialReadSchemaString
   * @return
   */
  public static MessageType getSchemaForRead(MessageType fileMessageType, String partialReadSchemaString) {
    if (partialReadSchemaString == null)
      return fileMessageType;
    MessageType requestedMessageType = MessageTypeParser.parseMessageType(partialReadSchemaString);
    return getSchemaForRead(fileMessageType, requestedMessageType);
  }

  /**
   * Updated method from ReadSupport which checks if the projection's compatible instead of a
   * stricter if the file's schema contains the projection
   *
   * @param fileMessageType
   * @param projectedMessageType
   * @return
   */
  public static MessageType getSchemaForRead(MessageType fileMessageType, MessageType projectedMessageType) {
    areGroupsCompatible(fileMessageType, projectedMessageType);
    return projectedMessageType;
  }

  /**
   * Method from ThriftReadSupport, copied because it was private
   */
  private void initThriftClassFromMultipleFiles(Map<String, Set<String>> fileMetadata, Configuration conf) throws ClassNotFoundException {
    if (thriftClass != null) {
      return;
    }
    String className = conf.get(THRIFT_READ_CLASS_KEY, null);
    if (className == null) {
      Set<String> names = ThriftMetaData.getThriftClassNames(fileMetadata);
      if (names == null || names.size() != 1) {
        throw new ParquetDecodingException("Could not read file as the Thrift class is not provided and could not be resolved from the file: " + names);
      }
      className = names.iterator().next();
    }
    thriftClass = (Class<T>) Class.forName(className);
  }

  /**
   * Validates that the requested group type projection is compatible.
   * This allows the projection schema to have extra optional fields.
   *
   * @param fileType
   * @param projection
   */
  public static void areGroupsCompatible(GroupType fileType, GroupType projection) {
    List<Type> fields = projection.getFields();
    for (Type otherType : fields) {
      if (fileType.containsField(otherType.getName())) {
        Type thisType = fileType.getType(otherType.getName());
        areCompatible(thisType, otherType);
        if (!otherType.isPrimitive()) {
          areGroupsCompatible(thisType.asGroupType(), otherType.asGroupType());
        }
      } else if (otherType.getRepetition() == Type.Repetition.REQUIRED) {
        throw new InvalidRecordException(otherType.getName() + " not found in " + fileType);
      }
    }
  }

  /**
   * Validates that the requested projection is compatible.
   * This makes it possible to project a required field using optional since it is less
   * restrictive.
   *
   * @param fileType
   * @param projection
   */
  public static void areCompatible(Type fileType, Type projection) {
    if (!fileType.getName().equals(projection.getName())
      || (fileType.getRepetition() != projection.getRepetition() && !fileType.getRepetition().isMoreRestrictiveThan(projection.getRepetition()))) {
      throw new InvalidRecordException(projection + " found: expected " + fileType);
    }
  }

}
