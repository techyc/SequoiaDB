/*
 * Copyright 2010-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sqldb.hadoop.io;


import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

/**
 * 
 * 
 * @className：BSONWritableComparator
 *
 * @author： gaoshengjie
 *
 * @createtime:2013年12月13日 上午11:05:29
 *
 * @changetime:TODO
 *
 * @version 1.0.0 
 *
 */
public class BSONWritableComparator extends WritableComparator {	
    private static final Log log = LogFactory.getLog( BSONWritableComparator.class );
    private static final Map<Class<?>, Integer> types;
    static {
        Map<Class<?>, Integer> aType = new HashMap<Class<?>, Integer>();
        aType.put(MinKey.class, 1);
        aType.put(null, 2);
        aType.put(Integer.class, 3);
        aType.put(Double.class, 3);
        aType.put(Float.class, 3);
        aType.put(String.class, 4);
        aType.put(Symbol.class, 4); 
        aType.put(BasicBSONObject.class, 5);
        aType.put(BasicBSONList.class, 6);
        aType.put(Binary.class, 7);
        aType.put(byte[].class, 7);
        aType.put(ObjectId.class, 8);
        aType.put(Boolean.class, 9);
        aType.put(Date.class, 10);
        aType.put(BSONTimestamp.class, 10);
        aType.put(Pattern.class, 11);
        aType.put(Code.class, 13);
        aType.put(CodeWScope.class, 13);
        aType.put(MaxKey.class, 12);
        types = aType;

    }

	

    public BSONWritableComparator(){
        super( BSONWritable.class, true );
    }

    protected BSONWritableComparator( Class<? extends WritableComparable> keyClass ){
        super( keyClass, true );
    }

    protected BSONWritableComparator( Class<? extends WritableComparable> keyClass, boolean createInstances ){
        super( keyClass, createInstances );
    }


    public int compare( WritableComparable a, WritableComparable b ){
        if ( a instanceof BSONWritable && b instanceof BSONWritable ){
            return compare(((BSONWritable)a).getBson(), ((BSONWritable)b).getBson());
        }else{
            return -1;
        }
    }

    public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 ){
        return super.compare( b1, s1, l1, b2, s2, l2 );
    }

    public int compare( Object a, Object b ){
        return compare(((BSONWritable)a).getBson(), ((BSONWritable)b).getBson());
    }
    
    private Iterator<Entry<String, Object>> getIterator(BSONObject obj) {
        
        if (obj instanceof BasicBSONObject) {
            return ((BasicBSONObject) obj).entrySet().iterator();
        } else {
            throw new IllegalArgumentException("only support BasicBSONObject");
        }
    }

    
    private  int compare(BSONObject obj1, BSONObject obj2) {

        Iterator<Entry<String, Object>> iter1 = getIterator(obj1);
        Iterator<Entry<String, Object>> iter2 = getIterator(obj2);
        
        while (iter1.hasNext()) {
            if (!iter2.hasNext()) {
                return -1;
            }
            
            Entry<String, Object> entry1 = iter1.next();
            Entry<String, Object> entry2 = iter2.next();
            
            int diff = entry1.getKey().compareTo(entry2.getKey());
            if (diff != 0) {
                return diff;
            }

            Object one = entry1.getValue();
            Object two = entry2.getValue();

            if (one == null && two == null) {
                continue;
            } 
            if (one == null && two != null) {
                return -1;
            }
            if (one != null && two == null) {
                return 1;
            } else {

                Integer oneValue = types.get(one.getClass());
                Integer twoValue = types.get(two.getClass());
                diff = oneValue.compareTo(twoValue);
                if (diff != 0) {
                    return diff;
                }

                diff = compareValues(one, two);

                if (diff != 0) {
                    return diff;
                }
            }

        }
        
        if (iter2.hasNext()) {
            return 1;
        }
        
        return 0;
    }
    
    private int compareValues(Object one, Object two) {
        int diff = 0;
        if (one instanceof Number) {
            diff = (Double.valueOf(one.toString())).
                    compareTo(Double.valueOf(two.toString()));
        } else if (one instanceof String) {
            diff = ((String) one).compareTo((String) two);
        } else if (one instanceof BSONObject) {
            diff = compare((BSONObject) one, (BSONObject)two);
        } else if (one instanceof Binary) {
            ByteBuffer buff1 = ByteBuffer.wrap(((Binary) one).getData());
            ByteBuffer buff2 = ByteBuffer.wrap(((Binary) two).getData());
            diff = buff1.compareTo(buff2);
        } else if (one instanceof byte[]) {
            ByteBuffer buff1 = ByteBuffer.wrap((byte[]) one);
            ByteBuffer buff2 = ByteBuffer.wrap((byte[]) two);
            diff = buff1.compareTo(buff2);
        } else if (one instanceof ObjectId) {
            diff = ((ObjectId) one).compareTo((ObjectId) two);
        } else if (one instanceof Boolean) {
            diff = ((Boolean) one).compareTo((Boolean) two);
        } else if (one instanceof Date) {
            diff = ((Date) one).compareTo((Date) two);
        } else if (one instanceof BSONTimestamp) {
            diff =compareBSONTimestamp((BSONTimestamp) one ,(BSONTimestamp) two);
        }
        

        return diff;
    }
    
    private int compareBSONTimestamp(BSONTimestamp left,BSONTimestamp right){
    	if(left.getTime()!=right.getTime()){
    		return left.getTime()-right.getTime();
    	}else{
    		return left.getInc()-right.getInc();
    	}
    }
}
