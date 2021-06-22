/*
 * Copyright (c) 2021. Increff
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.increff.commons.sql;

import org.apache.commons.dbcp2.BasicDataSource;

public class DbPoolUtil {

//    In general, 0 less than minIdle less than maxIdle less than maxTotal
//    Anything between maxIdle and maxTotal gets deleted automatically
//    To delete connections between minIdle and maxIdle, there are multiple eviction configuration
//    We want to avoid that confusion and just have a min and max connection
//
//    Hence for common util, we have following guidelines
//     1. MinIdle should not be set. It's default value is zero.
//     2. No need to set testWhileIdle & evictionThread properties because it works on MinIdle value.
//     3. MaxIdle is the number of connections that will always remain in the pool.
//     4. MaxTotal is the number of connections that can be created momentarily, but when connection is used,
//    total number of connections in the pool becomes equal to MaxIdle value.
//     5. Validation Query runs with each and every query to ensure connection is not broken.
//    NOTE: BasicDataSource Object passed in parameter is expected to only have JDBC driver, username, password, URL.
//    Example:
//    For heavy apps-  minConnection(MaxIdle, initialSize) = 30, maxConnection(MaxTotal) = 50
//    For light apps-  minConnection(MaxIdle, initialSize) = 10, maxConnection(MaxTotal) = 20
    public static BasicDataSource initDataSource(String driverClassName, String url, String userName, String password,
                                       int minConnection, int maxConnection) {
        BasicDataSource bean = new BasicDataSource();
        bean.setDriverClassName(driverClassName);
        bean.setUrl(url);
        bean.setUsername(userName);
        bean.setPassword(password);
        bean.setInitialSize(minConnection);
        bean.setMaxTotal(maxConnection);
        bean.setMaxIdle(minConnection);
        bean.setValidationQuery("SELECT 1");
        return bean;
    }
}
