/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.base;


import java.util.Map;

public abstract class ParamsUtil {

    @SuppressWarnings("unchecked")
    public static <T> T getParam(Map<?, ?> params, String key, Class<T> targetClass, T defaultValue) {
        Object param = params.get(key);
        if (param != null && targetClass.isAssignableFrom(param.getClass())) {
            return (T) param;
        } else {
            return defaultValue;
        }
    }

    public static String getString(Map<?, ?> params, String key, String defaultValue) {
        return getParam(params, key, String.class, defaultValue);
    }

    public static String getString(Map<?, ?> params, String key) {
        return getParam(params, key, String.class, null);
    }

    public static Integer getInt(Map<?, ?> params, String key, Integer defaultValue) {
        String stringParam = getString(params, key);
        if (stringParam == null) {
            return defaultValue;
        }
        return Integer.parseInt(stringParam);
    }

    public static Integer getInt(Map<?, ?> params, String key) {
        return getInt(params, key, null);
    }

    public static Long getLong(Map<?, ?> params, String key, Long defaultValue) {
        String stringParam = getString(params, key);
        if (stringParam == null) {
            return defaultValue;
        }
        return Long.parseLong(stringParam);
    }

    public static Long getLong(Map<?, ?> params, String key) {
        return getLong(params, key, null);
    }

}