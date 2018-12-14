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
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.apache.parquet.thrift.ThriftMetaData;
import org.apache.parquet.thrift.ThriftRecordConverter;
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
   * Method overridden from ThriftReadSupport to call
   * {@link #getSchemaForRead(MessageType, MessageType)} instead of
   * {@link ReadSupport#getSchemaForRead(MessageType, MessageType)}
   * <p>
   * The changes are done to fix use cases https://github.com/apache/parquet-mr/pull/558
   * Once that is merged, this overridden method can be removed along with
   * {@link #getSchemaForRead(MessageType, MessageType)}
   * {@link #getSchemaForRead(MessageType, String)}
   * {@link #assertAreCompatible(Type, Type)}
   * {@link #assertGroupsAreCompatible(GroupType, GroupType)}
   * {@link #getThriftClassFromMultipleFiles(Map, Configuration)}
   *
   * @param context the initialisation context
   * @return the readContext that defines how to read the file
   */
  @Override
  public ReadSupport.ReadContext init(InitContext context) {
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
    } else if (partialSchemaString != null) {
      requestedProjection = getSchemaForRead(fileMessageType, partialSchemaString);
    } else if (projectionFilter != null) {
      try {
        if (thriftClass == null) {
          thriftClass = getThriftClassFromMultipleFiles(context.getKeyValueMetadata(), configuration);
        }
        requestedProjection = getProjectedSchema(projectionFilter);
      } catch (ClassNotFoundException e) {
        throw new ThriftProjectionException("can not find thriftClass from configuration", e);
      }
    }

    MessageType schemaForRead = getSchemaForRead(fileMessageType, requestedProjection);
    return new ReadContext(schemaForRead);
  }

  /**
   * attempts to validate and construct a {@link MessageType} from a read projection schema
   *
   * @param fileMessageType         the typed schema of the source
   * @param partialReadSchemaString the requested projection schema
   * @return the typed schema that should be used to read
   */
  public static MessageType getSchemaForRead(MessageType fileMessageType, String partialReadSchemaString) {
    if (partialReadSchemaString == null)
      return fileMessageType;
    MessageType requestedMessageType = MessageTypeParser.parseMessageType(partialReadSchemaString);
    return getSchemaForRead(fileMessageType, requestedMessageType);
  }

  /**
   * Updated method from ReadSupport which checks if the projection's compatible instead of a
   * stricter check to see if the file's schema contains the projection
   *
   * @param fileMessageType
   * @param projectedMessageType
   * @return
   */
  public static MessageType getSchemaForRead(MessageType fileMessageType, MessageType projectedMessageType) {
    assertGroupsAreCompatible(fileMessageType, projectedMessageType);
    return projectedMessageType;
  }

  /**
   * Getting thrift class from file metadata
   */
  public static <T extends ThriftStruct> Class<T> getThriftClassFromMultipleFiles(Map<String, Set<String>> fileMetadata, Configuration conf) throws ClassNotFoundException {
    String className = conf.get(THRIFT_READ_CLASS_KEY, null);
    if (className == null) {
      Set<String> names = ThriftMetaData.getThriftClassNames(fileMetadata);
      if (names == null || names.size() != 1) {
        throw new ParquetDecodingException("Could not read file as the Thrift class is not provided and could not be resolved from the file: " + names);
      }
      className = names.iterator().next();
    }
    return (Class<T>) Class.forName(className);
  }

  /**
   * Validates that the requested group type projection is compatible.
   * This allows the projection schema to have extra optional fields.
   *
   * @param fileType   the typed schema of the source
   * @param projection requested projection schema
   */
  public static void assertGroupsAreCompatible(GroupType fileType, GroupType projection) {
    List<Type> fields = projection.getFields();
    for (Type otherType : fields) {
      if (fileType.containsField(otherType.getName())) {
        Type thisType = fileType.getType(otherType.getName());
        assertAreCompatible(thisType, otherType);
        if (!otherType.isPrimitive()) {
          assertGroupsAreCompatible(thisType.asGroupType(), otherType.asGroupType());
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
   * @param fileType   the typed schema of the source
   * @param projection requested projection schema
   */
  public static void assertAreCompatible(Type fileType, Type projection) {
    if (!fileType.getName().equals(projection.getName())
      || (fileType.getRepetition() != projection.getRepetition() && !fileType.getRepetition().isMoreRestrictiveThan(projection.getRepetition()))) {
      throw new InvalidRecordException(projection + " found: expected " + fileType);
    }
  }

  /**
   * Overriding to fall back to get descriptor from the {@link #thriftClass} if thrift metadata is
   * not present
   *
   * @return
   */
  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration,
                                              Map<String, String> keyValueMetaData, MessageType fileSchema,
                                              ReadSupport.ReadContext readContext) {
    ThriftMetaData thriftMetaData = ThriftMetaData.fromExtraMetaData(keyValueMetaData);
    try {
      if (thriftClass == null) {
        thriftClass = getThriftClass(keyValueMetaData, configuration);
      }

      ThriftType.StructType descriptor = null;
      if (thriftMetaData != null) {
        descriptor = thriftMetaData.getDescriptor();
      } else {
        ScroogeStructConverter schemaConverter = new ScroogeStructConverter();
        descriptor = schemaConverter.convert(thriftClass);
      }

      ThriftRecordConverter<T> converter = new ScroogeRecordConverter<T>(
        thriftClass,
        readContext.getRequestedSchema(),
        descriptor);
      return converter;
    } catch (Exception t) {
      throw new RuntimeException("Unable to create Thrift Converter for Thrift metadata " + thriftMetaData, t);
    }
  }

  /**
   * Getting thrift class from extra metadata
   */
  public static <T extends ThriftStruct> Class<T> getThriftClass(Map<String, String> fileMetadata, Configuration conf) throws ClassNotFoundException {
    String className = conf.get(THRIFT_READ_CLASS_KEY, null);
    if (className == null) {
      final ThriftMetaData metaData = ThriftMetaData.fromExtraMetaData(fileMetadata);
      if (metaData == null) {
        throw new ParquetDecodingException("Could not read file as the Thrift class is not provided and could not be resolved from the file");
      }
      return (Class<T>) metaData.getThriftClass();
    } else {
      return (Class<T>) Class.forName(className);
    }
  }

}
