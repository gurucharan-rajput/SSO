/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author SyedSadroddinSyedRah
 */
public class ConnectionStringUtil {

    static String dataSOURCE = "DATA SOURCE=";
    static String initialCatalog = "INITIAL CATALOG=";

    ConnectionStringUtil() {
    }

    /**
     * this method will take connectionString as input and return the
     * DatsourceOrDSn name back to the method calling
     *
     * @param connectionString
     * @return datasourceOrDSN String
     */
    @SuppressWarnings("all")
    public static String getDataSourceOrDSNFromConnectionString(String connectionString) {
        String dataSource = "";

        String stringInUpperCase = connectionString.toUpperCase();
        String dsn = "DSN=";

        try {
            if (stringInUpperCase.contains(dsn)) {
                dataSource = connectionString.substring(stringInUpperCase.indexOf(dsn) + 4, stringInUpperCase.indexOf(";", stringInUpperCase.indexOf(dsn)));
            } else if (stringInUpperCase.contains(dataSOURCE)) {
                dataSource = connectionString.substring(stringInUpperCase.indexOf(dataSOURCE) + 12, stringInUpperCase.indexOf(";", stringInUpperCase.indexOf(dataSOURCE)));
            } else {
                dataSource = connectionString;
            }

        } catch (Exception e) {
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getDataSourceOrDSNFromConnectionString(-,-)");
            e.printStackTrace();
        }
        return dataSource;
    }

    /**
     * this method will take connectionString as input and return the serverName
     * and databaseName back to the method calling
     *
     * @param connectionString
     * @return serverAndDatabaseName
     */
    public static String getDatabaseAndServerNameFromConnectionString(String connectionString) {
        String serverNameOrFilePath = "";
        String databaseName = "";
        String regularExpression = "[^a-zA-Z0-9]";

        String stringInUpperCase = connectionString.toUpperCase();

        try {
            if (stringInUpperCase.contains(dataSOURCE)) {
                serverNameOrFilePath = connectionString.substring(stringInUpperCase.indexOf(dataSOURCE) + 12, stringInUpperCase.indexOf(";", stringInUpperCase.indexOf(dataSOURCE)));
                serverNameOrFilePath = FilenameUtils.normalizeNoEndSeparator(serverNameOrFilePath, true);
                serverNameOrFilePath = serverNameOrFilePath.replaceAll("[^a-zA-Z0-9.//]", "_");
            } else {
                serverNameOrFilePath = connectionString;
            }

            if (stringInUpperCase.contains(initialCatalog) && connectionString.contains(";")) {

                int startIndex = stringInUpperCase.indexOf(initialCatalog) + 16;
                int endIndex = 0;
                if (stringInUpperCase.indexOf(";", startIndex) > 0) {
                    endIndex = stringInUpperCase.indexOf(";", stringInUpperCase.indexOf(initialCatalog));
                    databaseName = connectionString.substring(startIndex, endIndex);
                } else {
                    databaseName = connectionString.substring(startIndex);
                }

                databaseName = databaseName.replaceAll(regularExpression, "_");

            }
        } catch (Exception e) {
              MappingManagerUtilAutomation.writeExeceptionLog(e, "getDatabaseAndServerNameFromConnectionString(-,-)");
            e.printStackTrace();
        }

        String serverAndDbName = "";
        serverAndDbName = serverNameOrFilePath + Delimiter.delimiter + databaseName;
        return serverAndDbName;
    }

    /**
     * this method returns database and server name for a given connection
     * string
     *
     * @param connectionString
     * @return
     */
    public static String getDatabaseAndServerNameFromConnectionStringForOpenQuery(String connectionString) {
        String dataSource = "";
        String databaseName = "";

        String stringInUpperCase = connectionString.toUpperCase();

        try {
            if (stringInUpperCase.contains("DATA SOURCE=")) {
                dataSource = connectionString.substring(stringInUpperCase.indexOf("DATA SOURCE=") + 12, stringInUpperCase.indexOf(";", stringInUpperCase.indexOf("DATA SOURCE=")));
                dataSource = dataSource.replaceAll("[^a-zA-Z0-9]", "_");
            } else {
                dataSource = connectionString;
            }

            if (stringInUpperCase.contains("INITIAL CATALOG=") && connectionString.contains(";")) {
                databaseName = connectionString.substring(stringInUpperCase.indexOf("INITIAL CATALOG=") + 16, stringInUpperCase.indexOf(";", stringInUpperCase.indexOf("INITIAL CATALOG=")));
                databaseName = databaseName.replaceAll("[^a-zA-Z0-9]", "_");

            }
        } catch (Exception e) {
             MappingManagerUtilAutomation.writeExeceptionLog(e, "getDatabaseAndServerNameFromConnectionStringForOpenQuery(-,-)");

        }
        String dummyData = "test";
        String serverAndDbName = dataSource + Delimiter.delimiter + databaseName + Delimiter.delimiter + dummyData;
        return serverAndDbName;
    }

}
