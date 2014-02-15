/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

/**
 * A represenation of a schema used to communicate with load and store functions.  This is
 * separate from {@link Schema}, which is an internal Pig representation of a schema.
 * @since Pig 0.7
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@JsonPropertyOrder({ "fields", "version", "sortKeys", "sortKeyOrders" })
public class ResourceSchema implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private static Log log = LogFactory.getLog(ResourceSchema.class);
    
    /* Array Getters intentionally return mutable arrays instead of copies,
     * to simplify updates without unnecessary copying.
     * Setters make a copy of the arrays in order to prevent an array
     * from being shared by two objects, with modifications in one
     * accidentally changing the other.
     */
    
    // initializing arrays to empty so we don't have to worry about NPEs
    // setters won't set to null
    private ResourceFieldSchema[] fields = new ResourceFieldSchema[0];

    public enum Order { ASCENDING, DESCENDING }
    private int[] sortKeys = new int[0]; // each entry is an offset into the fields array.
    private Order[] sortKeyOrders = new Order[0];
        
    private int version = 0;

    @JsonPropertyOrder({ "name", "type", "description", "schema" })
    public static class ResourceFieldSchema implements Serializable {
        private static final long serialVersionUID = 1L;
        private String name;
        
        // values are constants from DataType
        private byte type;
        
        private String description;

        // nested tuples and bags will have their own schema
        private ResourceSchema schema; 

        /**
         * Construct an empty field schema.
         */
        public ResourceFieldSchema() {
            
        }
        
        /**
         * Construct using a {@link org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema} as the template.
         * @param fieldSchema fieldSchema to copy from
         */
        public ResourceFieldSchema(FieldSchema fieldSchema) {
            type = fieldSchema.type;
            name = fieldSchema.alias;
            description = "autogenerated from Pig Field Schema";
            Schema inner = fieldSchema.schema;
            
            // allow partial schema 
            if ((type == DataType.BAG || type == DataType.TUPLE || type == DataType.MAP)
                    && inner != null) {
                schema = new ResourceSchema(inner);
            } else {
                schema = null;
            }
        }
        
        /**
         * Construct using a {@link org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema} as the template.
         * @param fieldSchema fieldSchema to copy from
         */
        public ResourceFieldSchema(LogicalFieldSchema fieldSchema) {
            type = fieldSchema.type;
            name = fieldSchema.alias;
            description = "autogenerated from Pig Field Schema";
            LogicalSchema inner = fieldSchema.schema;

            // allow partial schema 
            if (DataType.isSchemaType(type) && inner != null) {
                schema = new ResourceSchema(inner);
            } else {
                schema = null;
            }
        }
        
        /**
         * Get the name of this field.
         * @return name
         */
        public String getName() {
            return name;
        }

        /**
         * Set the name of this filed.
         * @param name new name
         * @return this
         */
        public ResourceFieldSchema setName(String name) {
            this.name = name;
            return this;
        }
        
        /**
         * Get the type of this field.
         * @return type, as a {@link DataType} static final byte 
         */
        public byte getType() {
            return type;
        }
    
        /**
         * Set the type of this field
         * @param type new type
         * @return this
         */
        public ResourceFieldSchema setType(byte type) {
            this.type = type;
            return this;
        }
        
        /**
         * Get a free form text description of this field.
         * @return description
         */
        public String getDescription() {
            return description;
        }
     
        /**
         * Set the description
         * @param description new description
         * @return this
         */
        public ResourceFieldSchema setDescription(String description) {
            this.description = description;
            return this;
        }

        /**
         * Get the schema for this field. Type tuple/bag/map may have a schema.
         * @return schema
         */
        public ResourceSchema getSchema() {
            return schema;
        }

        /**
         * Set the schema for this field. Type tuple/bag/map may have a schema.
         * @param schema new schema
         * @return this
         */
        public ResourceFieldSchema setSchema(ResourceSchema schema) throws 
        IOException {
            validateSchema(schema);
            this.schema = schema;
            return this;
        }
                
        private void validateSchema(ResourceSchema schema) throws IOException {
            if(type == DataType.BAG && schema != null) {
                ResourceFieldSchema[] subFields = schema.getFields();
                if (subFields.length == 1) {
                    if (subFields[0].type != DataType.TUPLE) {
                        throwInvalidSchemaException();
                    }
                } else {
                    throwInvalidSchemaException();
                }
            }
        }
        
        public static void throwInvalidSchemaException() throws FrontendException {
            int errCode = 2218;
            throw new FrontendException("Invalid resource schema: " +
            "bag schema must have tuple as its field", errCode, PigException.BUG);   
        }

        @Override
        public String toString() {
            return getDescription(true);
        }
        
        public String calcCastString() {
            return getDescription(false);
        }
        
        private String getDescription(boolean printAlias) {
            StringBuilder sb = new StringBuilder();
            if (printAlias && this.name != null)
                sb.append(this.name).append(":");
            if (DataType.isAtomic(this.type)) {
                sb.append(DataType.findTypeName(this.type));
            } else {
                //if (this.schema!=null)
                    stringifyResourceSchema(sb, this.schema, this.type, printAlias);
            }
            return sb.toString();
        }
    }


    /**
     * Construct an empty ResourceSchema.
     */
    public ResourceSchema() {
        
    }
    
    /**
     * Construct a ResourceSchema from a {@link Schema}
     * @param pigSchema Schema to use
     */
    public ResourceSchema(Schema pigSchema) {
        List<FieldSchema> pigSchemaFields = pigSchema.getFields();
        fields = new ResourceFieldSchema[pigSchemaFields.size()];
        for (int i=0; i<fields.length; i++) {
            fields[i] = new ResourceFieldSchema(pigSchemaFields.get(i));
        }        
    }
    
    /**
     * Construct a ResourceSchema from a {@link LogicalSchema}
     * @param pigSchema Schema to use
     */
    public ResourceSchema(LogicalSchema pigSchema) {
        List<LogicalFieldSchema> pigSchemaFields = pigSchema.getFields();
        fields = new ResourceFieldSchema[pigSchemaFields.size()];
        for (int i=0; i<fields.length; i++) {
            fields[i] = new ResourceFieldSchema(pigSchemaFields.get(i));
        }        
    }
    
    /**
     * Only for use by Pig internal code.
     * Construct a ResourceSchema from a {@link Schema}
     * @param pigSchema Schema to use
     * @param sortInfo information on how data is sorted
     */
    @InterfaceAudience.Private
    public ResourceSchema(Schema pigSchema, SortInfo sortInfo) {
        this(pigSchema);
        if (sortInfo!=null && sortInfo.getSortColInfoList().size()!=0) {
            sortKeys = new int[sortInfo.getSortColInfoList().size()];
            sortKeyOrders = new Order[sortInfo.getSortColInfoList().size()];
            for (int i=0;i<sortInfo.getSortColInfoList().size();i++) {
                SortColInfo colInfo = sortInfo.getSortColInfoList().get(i); 
                int index = colInfo.getColIndex();
                Order order;
                org.apache.pig.SortColInfo.Order origOrder = colInfo.getSortOrder();
                if (origOrder==org.apache.pig.SortColInfo.Order.ASCENDING) {
                    order = Order.ASCENDING;
                } else {
                    order = Order.DESCENDING;
                }
                sortKeys[i] = index;
                sortKeyOrders[i] = order;
            }
        }
    }
    
    /**
     * Only for use by Pig internal code.
     * Construct a ResourceSchema from a {@link LogicalSchema}
     * @param pigSchema LogicalSchema to use
     * @param sortInfo information on how data is sorted
     */
    @InterfaceAudience.Private
    public ResourceSchema(LogicalSchema pigSchema, SortInfo sortInfo) {
        this(pigSchema);
        if (sortInfo!=null && sortInfo.getSortColInfoList().size()!=0) {
            sortKeys = new int[sortInfo.getSortColInfoList().size()];
            sortKeyOrders = new Order[sortInfo.getSortColInfoList().size()];
            for (int i=0;i<sortInfo.getSortColInfoList().size();i++) {
                SortColInfo colInfo = sortInfo.getSortColInfoList().get(i); 
                int index = colInfo.getColIndex();
                Order order;
                org.apache.pig.SortColInfo.Order origOrder = colInfo.getSortOrder();
                if (origOrder==org.apache.pig.SortColInfo.Order.ASCENDING) {
                    order = Order.ASCENDING;
                } else {
                    order = Order.DESCENDING;
                }
                sortKeys[i] = index;
                sortKeyOrders[i] = order;
            }
        }
    }
    
    /**
     * Get the version of this schema.
     * @return version
     */
    public int getVersion() {
        return version;
    }
    
    public ResourceSchema setVersion(int version) {
        this.version = version;
        return this;
    }
    
    /**
     * Get field schema for each field
     * @return array of field schemas.
     */
    public ResourceFieldSchema[] getFields() {
        return fields;
    }
    
    /**
     * Get all field names.
     * @return array of field names
     */
    public String[] fieldNames() {
        String[] names = new String[fields.length];
        for (int i=0; i<fields.length; i++) {
            names[i] = fields[i].getName();
        }
        return names;
    }
    
    /**
     * Set all the fields.  If fields are not currently null the new fields will be silently
     * ignored.
     * @param fields to use as fields in this schema
     * @return this
     */
    public ResourceSchema setFields(ResourceFieldSchema[] fields) {
        if (fields != null)
            this.fields = Arrays.copyOf(fields, fields.length);
        return this;
    }
    
    /**
     * Get the sort keys for this data.
     * @return array of ints.  Each integer in the array represents the field number.  So if the
     * schema of the data is (a, b, c, d) and the data is sorted on c, b, the returned sort keys
     * will be [2, 1].  Field numbers are zero based.  If the data is not sorted a zero length
     * array will be returned.
     */
    public int[] getSortKeys() {
        return sortKeys;
    }
    
    /**
     * Set the sort keys for htis data.  If sort keys are not currently null the new sort keys
     * will be silently ignored.
     * @param sortKeys  Each integer in the array represents the field number.  So if the
     * schema of the data is (a, b, c, d) and the data is sorted on c, b, the sort keys
     * should be [2, 1].  Field numbers are zero based.
     * @return this
     */
    public  ResourceSchema setSortKeys(int[] sortKeys) {
        if (sortKeys != null)
            this.sortKeys = Arrays.copyOf(sortKeys, sortKeys.length);
        return this;
    }
    
    /**
     * Get order for sort keys.
     * @return array of Order.  This array will be the same length as the int[] array returned by
     * {@link #getSortKeys}.
     */
    public Order[] getSortKeyOrders() {
        return sortKeyOrders;
    }
    
    /**
     * Set the order for each sort key.  If order is not currently null, new order will be
     * silently ignored.
     * @param sortKeyOrders array of Order.  Should be the same length as int[] passed to 
     * {@link #setSortKeys}.
     * @return this
     */
    public ResourceSchema setSortKeyOrders(Order[] sortKeyOrders) {
        if (sortKeyOrders != null) 
            this.sortKeyOrders = Arrays.copyOf(sortKeyOrders, sortKeyOrders.length);
        return this;
    } 
            
    /**
     * Test whether two ResourceSchemas are the same.  Two schemas are said to be the same if they
     * are both null or 
     * have the same number of fields and for each field the name, type are the same.  For fields
     * that have may have schemas (i.e. tuples) both schemas be equal.  Field
     * descriptions are not used in testing equality.
     * @return true if equal according to the above definition, otherwise false.
     */
    public static boolean equals(ResourceSchema rs1, ResourceSchema rs2) {
        if (rs1 == null) {
            return rs2 == null ? true : false;
        }
        
        if (rs2 == null) {
            return false;
        }
        
        if (rs1.getVersion() != rs2.getVersion() 
                || !Arrays.equals(rs1.getSortKeys(), rs2.getSortKeys())
                || !Arrays.equals(rs1.getSortKeyOrders(), rs2.getSortKeyOrders())) {            
            return false;
        }            
        
        ResourceFieldSchema[] rfs1 = rs1.getFields();
        ResourceFieldSchema[] rfs2 = rs1.getFields();
        
        if (rfs1.length != rfs2.length) return false;
        
        for (int i=0; i<rfs1.length; i++) {
            if (rfs1[i].getName()==null && rfs2[i].getName()!=null ||
                    rfs1[i].getName()!=null && rfs2[i].getName()==null)
                return false;
            if (rfs1[i].getName()==null && rfs2[i].getName()==null) {
                if (rfs1[i].getType() == rfs2[i].getType())
                    return true;
                else
                    return false;
            }
            if (!rfs1[i].getName().equals(rfs2[i].getName()) 
                    || rfs1[i].getType() != rfs2[i].getType()) {
                return false;
            }
            if (!equals(rfs1[i].getSchema(), rfs2[i].getSchema())) {
                return false;
            } 
        }
        
        return true;
    }
      
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        stringifyResourceSchema(sb, this, DataType.UNKNOWN, true) ;
        return sb.toString();
    }
    
    private static void stringifyResourceSchema(StringBuilder sb, 
            ResourceSchema rs, byte type, boolean printAlias) {

        if (type == DataType.BAG) {
            sb.append("{");
        } else if (type == DataType.TUPLE) {
            sb.append("(");
        } else if (type == DataType.MAP) {
            sb.append("[");
        }
        
        if (rs != null) {
            for (int i=0; i<rs.getFields().length; i++) {
                sb.append(rs.getFields()[i].getDescription(printAlias));
                if (i < rs.getFields().length - 1) {
                    sb.append(",");
                }
            }
        }
                
        if (type == DataType.BAG) {
            sb.append("}");
        } else if (type == DataType.TUPLE) {
            sb.append(")");
        } else if (type == DataType.MAP) {
            sb.append("]");
        }
    }
}
