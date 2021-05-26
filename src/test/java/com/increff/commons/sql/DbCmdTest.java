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

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DbCmdTest {

    @Ignore
    @Test
    public void testDbCmd() throws DbException, IOException {
        DbCmd controller = new DbCmd("src/test/resources", "127.0.0.1",
                "***", "***", "testDbNew");

        // Delete the schema (if already exists
        controller.dropDb("testDbNew");

        // Create a Schema with name testDbNew
        controller.createDb("testDbNew");
        try {
            controller.createDb("testDbNew");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        // Create a new table in the database
        controller.processQuery("CREATE TABLE test_table (name VARCHAR(50), pincode INT);");

        // Insert data into table
        controller.processQuery("INSERT INTO test_table VALUES ('Alice', 190006); ");
        controller.processQuery("INSERT INTO test_table VALUES ('John', 7800611); ");

        // Pull data into file
        controller.pull("dbresult.txt", "SELECT * FROM test_table");
        // Test pulled data
        File expected = new File("src/test/resources/dbresultExpected.txt");
        File actual = new File("src/test/resources/dbresult.txt");
        assertTrue(expected.exists());
        assertTrue(actual.exists());
        assertTrue("The files differ!", FileUtils.contentEquals(expected, actual));

        // Truncate table data
        controller.truncate("test_table");

        // Delete the schema
        controller.dropDb("testDbNew");
    }

}
