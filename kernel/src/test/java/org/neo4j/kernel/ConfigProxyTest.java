/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel;

import java.util.HashMap;
import java.util.Map;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * ConfigProxy tests
 */
public class ConfigProxyTest
{
    public interface Configuration
    {
        float floatNoDef();
        float floatValueMinMax(float def, float min, float max);
        double doubleValueMinMax(double def, double min, double max);
        long longValueMinMax(long def, long min, long max);
        int integerValueMinMax(int def, int min, int max);
        
        boolean boolDefined();
        boolean boolNotDefined();
        boolean boolNotDefinedWithDefault(boolean def);
    }
    
    @Test
    public void testNumbersAndRanges()
    {
        Map<String,String> map = new HashMap<String, String>();
        map.put("floatValueMinMax", "3.0");
        map.put("doubleValueMinMax", "3.0");
        map.put("longValueMinMax", "3");
        map.put("integerValueMinMax", "3");
        
        Configuration conf = ConfigProxy.config(map, Configuration.class);
        Assert.assertThat(conf.floatValueMinMax(4, 1, 5), CoreMatchers.equalTo(3.0F));
        Assert.assertThat(conf.floatValueMinMax(4, 5, 7), CoreMatchers.equalTo(5.0F));
        Assert.assertThat(conf.floatValueMinMax(4, 1, 2), CoreMatchers.equalTo(2.0F));

        Assert.assertThat(conf.doubleValueMinMax(4, 1, 5), CoreMatchers.equalTo(3.0D));
        Assert.assertThat(conf.doubleValueMinMax(4, 5, 7), CoreMatchers.equalTo(5.0D));
        Assert.assertThat(conf.doubleValueMinMax(4, 1, 2), CoreMatchers.equalTo(2.0D));

        Assert.assertThat(conf.longValueMinMax(4, 1, 5), CoreMatchers.equalTo(3L));
        Assert.assertThat(conf.longValueMinMax(4, 5, 7), CoreMatchers.equalTo(5L));
        Assert.assertThat(conf.longValueMinMax(4, 1, 2), CoreMatchers.equalTo(2L));

        Assert.assertThat(conf.integerValueMinMax(4, 1, 5), CoreMatchers.equalTo(3));
        Assert.assertThat(conf.integerValueMinMax(4, 5, 7), CoreMatchers.equalTo(5));
        Assert.assertThat(conf.integerValueMinMax(4, 1, 2), CoreMatchers.equalTo(2));

        // Invalid number format
        map.put("floatValueMinMax", "3x");
        Assert.assertThat(conf.floatValueMinMax(4, 1, 5), CoreMatchers.equalTo(4.0F));
        map.put("floatValueMinMax", "3,0");
        Assert.assertThat(conf.floatValueMinMax(4, 1, 5), CoreMatchers.equalTo(4.0F));
        map.put("floatNoDef", "3,0");
        try
        {
            conf.floatNoDef();
            Assert.fail("Should have thrown exception");
        } catch (Exception e)
        {
            // Ok!
        }
    }
    
    @Test
    public void testBoolean()
    {
        Map<String,String> map = new HashMap<String, String>();
        map.put("boolDefined", "true");
        
        Configuration conf = ConfigProxy.config(map, Configuration.class);
        Assert.assertThat(conf.boolDefined(), CoreMatchers.equalTo(true));
        map.put("boolDefined", "TrUe");
        Assert.assertThat(conf.boolDefined(), CoreMatchers.equalTo(true));

        try
        {
            conf.boolNotDefined();
            Assert.fail();
        } catch (Exception e)
        {
        }

        Assert.assertThat(conf.boolNotDefinedWithDefault(true), CoreMatchers.equalTo(true));
    }
}
