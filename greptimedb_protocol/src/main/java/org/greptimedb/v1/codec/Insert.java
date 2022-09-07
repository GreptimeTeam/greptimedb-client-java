// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: insert.proto

package org.greptimedb.v1.codec;

public final class Insert {
  private Insert() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface InsertBatchOrBuilder extends
      // @@protoc_insertion_point(interface_extends:greptime.v1.codec.InsertBatch)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    java.util.List<org.greptimedb.v1.Columns.Column> 
        getColumnsList();
    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    org.greptimedb.v1.Columns.Column getColumns(int index);
    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    int getColumnsCount();
    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    java.util.List<? extends org.greptimedb.v1.Columns.ColumnOrBuilder> 
        getColumnsOrBuilderList();
    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    org.greptimedb.v1.Columns.ColumnOrBuilder getColumnsOrBuilder(
        int index);

    /**
     * <code>uint32 row_count = 2;</code>
     * @return The rowCount.
     */
    int getRowCount();
  }
  /**
   * Protobuf type {@code greptime.v1.codec.InsertBatch}
   */
  public static final class InsertBatch extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:greptime.v1.codec.InsertBatch)
      InsertBatchOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use InsertBatch.newBuilder() to construct.
    private InsertBatch(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private InsertBatch() {
      columns_ = java.util.Collections.emptyList();
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new InsertBatch();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private InsertBatch(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              if (!((mutable_bitField0_ & 0x00000001) != 0)) {
                columns_ = new java.util.ArrayList<org.greptimedb.v1.Columns.Column>();
                mutable_bitField0_ |= 0x00000001;
              }
              columns_.add(
                  input.readMessage(org.greptimedb.v1.Columns.Column.parser(), extensionRegistry));
              break;
            }
            case 16: {

              rowCount_ = input.readUInt32();
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000001) != 0)) {
          columns_ = java.util.Collections.unmodifiableList(columns_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.greptimedb.v1.codec.Insert.internal_static_greptime_v1_codec_InsertBatch_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.greptimedb.v1.codec.Insert.internal_static_greptime_v1_codec_InsertBatch_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.greptimedb.v1.codec.Insert.InsertBatch.class, org.greptimedb.v1.codec.Insert.InsertBatch.Builder.class);
    }

    public static final int COLUMNS_FIELD_NUMBER = 1;
    private java.util.List<org.greptimedb.v1.Columns.Column> columns_;
    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    @java.lang.Override
    public java.util.List<org.greptimedb.v1.Columns.Column> getColumnsList() {
      return columns_;
    }
    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    @java.lang.Override
    public java.util.List<? extends org.greptimedb.v1.Columns.ColumnOrBuilder> 
        getColumnsOrBuilderList() {
      return columns_;
    }
    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    @java.lang.Override
    public int getColumnsCount() {
      return columns_.size();
    }
    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    @java.lang.Override
    public org.greptimedb.v1.Columns.Column getColumns(int index) {
      return columns_.get(index);
    }
    /**
     * <code>repeated .greptime.v1.Column columns = 1;</code>
     */
    @java.lang.Override
    public org.greptimedb.v1.Columns.ColumnOrBuilder getColumnsOrBuilder(
        int index) {
      return columns_.get(index);
    }

    public static final int ROW_COUNT_FIELD_NUMBER = 2;
    private int rowCount_;
    /**
     * <code>uint32 row_count = 2;</code>
     * @return The rowCount.
     */
    @java.lang.Override
    public int getRowCount() {
      return rowCount_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      for (int i = 0; i < columns_.size(); i++) {
        output.writeMessage(1, columns_.get(i));
      }
      if (rowCount_ != 0) {
        output.writeUInt32(2, rowCount_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      for (int i = 0; i < columns_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, columns_.get(i));
      }
      if (rowCount_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeUInt32Size(2, rowCount_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.greptimedb.v1.codec.Insert.InsertBatch)) {
        return super.equals(obj);
      }
      org.greptimedb.v1.codec.Insert.InsertBatch other = (org.greptimedb.v1.codec.Insert.InsertBatch) obj;

      if (!getColumnsList()
          .equals(other.getColumnsList())) return false;
      if (getRowCount()
          != other.getRowCount()) return false;
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (getColumnsCount() > 0) {
        hash = (37 * hash) + COLUMNS_FIELD_NUMBER;
        hash = (53 * hash) + getColumnsList().hashCode();
      }
      hash = (37 * hash) + ROW_COUNT_FIELD_NUMBER;
      hash = (53 * hash) + getRowCount();
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.greptimedb.v1.codec.Insert.InsertBatch parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(org.greptimedb.v1.codec.Insert.InsertBatch prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code greptime.v1.codec.InsertBatch}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:greptime.v1.codec.InsertBatch)
        org.greptimedb.v1.codec.Insert.InsertBatchOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.greptimedb.v1.codec.Insert.internal_static_greptime_v1_codec_InsertBatch_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.greptimedb.v1.codec.Insert.internal_static_greptime_v1_codec_InsertBatch_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.greptimedb.v1.codec.Insert.InsertBatch.class, org.greptimedb.v1.codec.Insert.InsertBatch.Builder.class);
      }

      // Construct using org.greptimedb.v1.codec.Insert.InsertBatch.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
          getColumnsFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (columnsBuilder_ == null) {
          columns_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          columnsBuilder_.clear();
        }
        rowCount_ = 0;

        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.greptimedb.v1.codec.Insert.internal_static_greptime_v1_codec_InsertBatch_descriptor;
      }

      @java.lang.Override
      public org.greptimedb.v1.codec.Insert.InsertBatch getDefaultInstanceForType() {
        return org.greptimedb.v1.codec.Insert.InsertBatch.getDefaultInstance();
      }

      @java.lang.Override
      public org.greptimedb.v1.codec.Insert.InsertBatch build() {
        org.greptimedb.v1.codec.Insert.InsertBatch result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.greptimedb.v1.codec.Insert.InsertBatch buildPartial() {
        org.greptimedb.v1.codec.Insert.InsertBatch result = new org.greptimedb.v1.codec.Insert.InsertBatch(this);
        int from_bitField0_ = bitField0_;
        if (columnsBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)) {
            columns_ = java.util.Collections.unmodifiableList(columns_);
            bitField0_ = (bitField0_ & ~0x00000001);
          }
          result.columns_ = columns_;
        } else {
          result.columns_ = columnsBuilder_.build();
        }
        result.rowCount_ = rowCount_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.greptimedb.v1.codec.Insert.InsertBatch) {
          return mergeFrom((org.greptimedb.v1.codec.Insert.InsertBatch)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.greptimedb.v1.codec.Insert.InsertBatch other) {
        if (other == org.greptimedb.v1.codec.Insert.InsertBatch.getDefaultInstance()) return this;
        if (columnsBuilder_ == null) {
          if (!other.columns_.isEmpty()) {
            if (columns_.isEmpty()) {
              columns_ = other.columns_;
              bitField0_ = (bitField0_ & ~0x00000001);
            } else {
              ensureColumnsIsMutable();
              columns_.addAll(other.columns_);
            }
            onChanged();
          }
        } else {
          if (!other.columns_.isEmpty()) {
            if (columnsBuilder_.isEmpty()) {
              columnsBuilder_.dispose();
              columnsBuilder_ = null;
              columns_ = other.columns_;
              bitField0_ = (bitField0_ & ~0x00000001);
              columnsBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getColumnsFieldBuilder() : null;
            } else {
              columnsBuilder_.addAllMessages(other.columns_);
            }
          }
        }
        if (other.getRowCount() != 0) {
          setRowCount(other.getRowCount());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.greptimedb.v1.codec.Insert.InsertBatch parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.greptimedb.v1.codec.Insert.InsertBatch) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.util.List<org.greptimedb.v1.Columns.Column> columns_ =
        java.util.Collections.emptyList();
      private void ensureColumnsIsMutable() {
        if (!((bitField0_ & 0x00000001) != 0)) {
          columns_ = new java.util.ArrayList<org.greptimedb.v1.Columns.Column>(columns_);
          bitField0_ |= 0x00000001;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.greptimedb.v1.Columns.Column, org.greptimedb.v1.Columns.Column.Builder, org.greptimedb.v1.Columns.ColumnOrBuilder> columnsBuilder_;

      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public java.util.List<org.greptimedb.v1.Columns.Column> getColumnsList() {
        if (columnsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(columns_);
        } else {
          return columnsBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public int getColumnsCount() {
        if (columnsBuilder_ == null) {
          return columns_.size();
        } else {
          return columnsBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public org.greptimedb.v1.Columns.Column getColumns(int index) {
        if (columnsBuilder_ == null) {
          return columns_.get(index);
        } else {
          return columnsBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public Builder setColumns(
          int index, org.greptimedb.v1.Columns.Column value) {
        if (columnsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureColumnsIsMutable();
          columns_.set(index, value);
          onChanged();
        } else {
          columnsBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public Builder setColumns(
          int index, org.greptimedb.v1.Columns.Column.Builder builderForValue) {
        if (columnsBuilder_ == null) {
          ensureColumnsIsMutable();
          columns_.set(index, builderForValue.build());
          onChanged();
        } else {
          columnsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public Builder addColumns(org.greptimedb.v1.Columns.Column value) {
        if (columnsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureColumnsIsMutable();
          columns_.add(value);
          onChanged();
        } else {
          columnsBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public Builder addColumns(
          int index, org.greptimedb.v1.Columns.Column value) {
        if (columnsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureColumnsIsMutable();
          columns_.add(index, value);
          onChanged();
        } else {
          columnsBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public Builder addColumns(
          org.greptimedb.v1.Columns.Column.Builder builderForValue) {
        if (columnsBuilder_ == null) {
          ensureColumnsIsMutable();
          columns_.add(builderForValue.build());
          onChanged();
        } else {
          columnsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public Builder addColumns(
          int index, org.greptimedb.v1.Columns.Column.Builder builderForValue) {
        if (columnsBuilder_ == null) {
          ensureColumnsIsMutable();
          columns_.add(index, builderForValue.build());
          onChanged();
        } else {
          columnsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public Builder addAllColumns(
          java.lang.Iterable<? extends org.greptimedb.v1.Columns.Column> values) {
        if (columnsBuilder_ == null) {
          ensureColumnsIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, columns_);
          onChanged();
        } else {
          columnsBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public Builder clearColumns() {
        if (columnsBuilder_ == null) {
          columns_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
          onChanged();
        } else {
          columnsBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public Builder removeColumns(int index) {
        if (columnsBuilder_ == null) {
          ensureColumnsIsMutable();
          columns_.remove(index);
          onChanged();
        } else {
          columnsBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public org.greptimedb.v1.Columns.Column.Builder getColumnsBuilder(
          int index) {
        return getColumnsFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public org.greptimedb.v1.Columns.ColumnOrBuilder getColumnsOrBuilder(
          int index) {
        if (columnsBuilder_ == null) {
          return columns_.get(index);  } else {
          return columnsBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public java.util.List<? extends org.greptimedb.v1.Columns.ColumnOrBuilder> 
           getColumnsOrBuilderList() {
        if (columnsBuilder_ != null) {
          return columnsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(columns_);
        }
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public org.greptimedb.v1.Columns.Column.Builder addColumnsBuilder() {
        return getColumnsFieldBuilder().addBuilder(
            org.greptimedb.v1.Columns.Column.getDefaultInstance());
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public org.greptimedb.v1.Columns.Column.Builder addColumnsBuilder(
          int index) {
        return getColumnsFieldBuilder().addBuilder(
            index, org.greptimedb.v1.Columns.Column.getDefaultInstance());
      }
      /**
       * <code>repeated .greptime.v1.Column columns = 1;</code>
       */
      public java.util.List<org.greptimedb.v1.Columns.Column.Builder> 
           getColumnsBuilderList() {
        return getColumnsFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.greptimedb.v1.Columns.Column, org.greptimedb.v1.Columns.Column.Builder, org.greptimedb.v1.Columns.ColumnOrBuilder> 
          getColumnsFieldBuilder() {
        if (columnsBuilder_ == null) {
          columnsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.greptimedb.v1.Columns.Column, org.greptimedb.v1.Columns.Column.Builder, org.greptimedb.v1.Columns.ColumnOrBuilder>(
                  columns_,
                  ((bitField0_ & 0x00000001) != 0),
                  getParentForChildren(),
                  isClean());
          columns_ = null;
        }
        return columnsBuilder_;
      }

      private int rowCount_ ;
      /**
       * <code>uint32 row_count = 2;</code>
       * @return The rowCount.
       */
      @java.lang.Override
      public int getRowCount() {
        return rowCount_;
      }
      /**
       * <code>uint32 row_count = 2;</code>
       * @param value The rowCount to set.
       * @return This builder for chaining.
       */
      public Builder setRowCount(int value) {
        
        rowCount_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>uint32 row_count = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearRowCount() {
        
        rowCount_ = 0;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:greptime.v1.codec.InsertBatch)
    }

    // @@protoc_insertion_point(class_scope:greptime.v1.codec.InsertBatch)
    private static final org.greptimedb.v1.codec.Insert.InsertBatch DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.greptimedb.v1.codec.Insert.InsertBatch();
    }

    public static org.greptimedb.v1.codec.Insert.InsertBatch getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<InsertBatch>
        PARSER = new com.google.protobuf.AbstractParser<InsertBatch>() {
      @java.lang.Override
      public InsertBatch parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new InsertBatch(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<InsertBatch> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<InsertBatch> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.greptimedb.v1.codec.Insert.InsertBatch getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_greptime_v1_codec_InsertBatch_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_greptime_v1_codec_InsertBatch_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\014insert.proto\022\021greptime.v1.codec\032\014colum" +
      "n.proto\"F\n\013InsertBatch\022$\n\007columns\030\001 \003(\0132" +
      "\023.greptime.v1.Column\022\021\n\trow_count\030\002 \001(\rB" +
      "!\n\027org.greptimedb.v1.codecB\006Insertb\006prot" +
      "o3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          org.greptimedb.v1.Columns.getDescriptor(),
        });
    internal_static_greptime_v1_codec_InsertBatch_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_greptime_v1_codec_InsertBatch_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_greptime_v1_codec_InsertBatch_descriptor,
        new java.lang.String[] { "Columns", "RowCount", });
    org.greptimedb.v1.Columns.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
