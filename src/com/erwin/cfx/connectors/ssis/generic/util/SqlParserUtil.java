/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.beans.DataFlowBean;
import com.erwin.sqlparser.DDLQueryGeneratorV1;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 26-08-2021
 */
public class SqlParserUtil {

    /**
     * this method returns EDbVendor for a given database name
     *
     * @param dbName
     * @return
     */
    public static EDbVendor getDBVendorFromStringVendorName(String dbName) {
        EDbVendor dbVendor = EDbVendor.dbvmssql;
        if (!StringUtils.isBlank(dbName)) {
            dbName = dbName.toLowerCase();
            switch (dbName) {
                case "oracle":
                    dbVendor = EDbVendor.dbvoracle;
                    break;
                case "mssql":
                    dbVendor = EDbVendor.dbvmssql;
                    break;
                case "postgresql":
                    dbVendor = EDbVendor.dbvpostgresql;
                    break;
                case "redshift":
                    dbVendor = EDbVendor.dbvredshift;
                    break;
                case "odbc":
                    dbVendor = EDbVendor.dbvodbc;
                    break;
                case "mysql":
                    dbVendor = EDbVendor.dbvmysql;
                    break;
                case "netezza":
                    dbVendor = EDbVendor.dbvnetezza;
                    break;
                case "firebird":
                    dbVendor = EDbVendor.dbvfirebird;
                    break;
                case "access":
                    dbVendor = EDbVendor.dbvaccess;
                    break;
                case "ansi":
                    dbVendor = EDbVendor.dbvansi;
                    break;
                case "generic":
                    dbVendor = EDbVendor.dbvgeneric;
                    break;
                case "greenplum":
                    dbVendor = EDbVendor.dbvgreenplum;
                    break;
                case "hive":
                    dbVendor = EDbVendor.dbvhive;
                    break;
                case "sybase":
                    dbVendor = EDbVendor.dbvsybase;
                    break;
                case "hana":
                    dbVendor = EDbVendor.dbvhana;
                    break;
                case "impala":
                    dbVendor = EDbVendor.dbvimpala;
                    break;
                case "dax":
                    dbVendor = EDbVendor.dbvdax;
                    break;
                case "vertica":
                    dbVendor = EDbVendor.dbvvertica;
                    break;
                case "couchbase":
                    dbVendor = EDbVendor.dbvcouchbase;
                    break;
                case "snowflake":
                    dbVendor = EDbVendor.dbvsnowflake;
                    break;
                case "openedge":
                    dbVendor = EDbVendor.dbvopenedge;
                    break;
                case "informix":
                    dbVendor = EDbVendor.dbvinformix;
                    break;
                case "teradata":
                    dbVendor = EDbVendor.dbvteradata;
                    break;
                case "mdx":
                    dbVendor = EDbVendor.dbvmdx;
                    break;
                case "db2":
                    dbVendor = EDbVendor.dbvdb2;
                    break;
                default:
                    break;
            }
        }

        return dbVendor;
    }

    public static TGSqlParser isQueryParsable(String sqltext, EDbVendor vendor) {
        TGSqlParser sqlparser = new TGSqlParser(vendor);

        sqlparser.sqltext = sqltext;
        int parsedResult = sqlparser.parse();

        if (parsedResult == 0) {
            return sqlparser;
        } else {
            return null;
        }
    }

    /**
     * this method returns the stored procedure name for a given query
     *
     * @param query
     * @param componentName
     * @return
     */
    public static String makeStorePrcedureNameAsComponentName(String query, String componentName) {

        try {
            if (!StringUtils.isBlank(query)) {
                query = query.trim();
            }

            if (query.toUpperCase().startsWith("EXEC") || query.toUpperCase().startsWith("EXECUTE")) {

                componentName = getStoreProcNameFromExecuteCommand(query) + Delimiter.storeProcdelimiter;

//                query = "";
            } else if (query.toUpperCase().contains("EXECUTE ") || query.toUpperCase().contains("EXEC ")) {
                String querySubString = null;

                if (query.toUpperCase().contains("EXECUTE")) {
                    querySubString = StringUtils.substring(query, query.toUpperCase().indexOf("EXECUTE"));
                } else if (query.toUpperCase().contains("EXEC")) {
                    querySubString = StringUtils.substring(query, query.toUpperCase().indexOf("EXEC"));
                }
                componentName = getStoreProcNameFromExecuteCommand(querySubString) + Delimiter.storeProcdelimiter;

            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "makeStorePrcedureNameAsComponentName(-,-)");
        }
        return query + Delimiter.delimiter + componentName;
    }

    /**
     * this method returns table name extracting all the unwanted data from the
     * given query
     *
     * @param query
     * @return
     */
    public static String getStoreProcNameFromExecuteCommand(String query) {
        String key = "";
        try {
            query = query.replace("\n", " ");
            String querySpilt[] = query.split(" ");

            for (String queryData : querySpilt) {
                if (!StringUtils.isBlank(queryData) && !(queryData.equalsIgnoreCase("EXECUTE") || queryData.equalsIgnoreCase("EXEC"))) {
                    key = queryData;
                    key = key.replace("[", "").replace("]", "").trim();
                    if (StringUtils.isNotBlank(key)) {
                        key = key.replace("'", "").replace("\\?", "").replace("EXEC", "").replace("EXECUTE", "").replace("exec", "").replace("execute", "");
                    }
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getStoreProcNameFromExecuteCommand(-,-)");
        }

        return key;
    }

    /**
     * this method updates the query map with the updated columns if query
     * contains
     *
     * @param query
     *
     * @param dataBaseName
     * @param assignedTotalOutPutColumns
     * @param returnedTotalInputColumns
     * @param queryMap
     * @param executableName
     * @param queryMapKeyComponentName
     * @param serverName
     * @param dataFlowBean
     */
    public static String updateQueryIfContainsStarInIt(String query, String dataBaseName, String assignedTotalOutPutColumns, String returnedTotalInputColumns, Map<String, String> queryMap, String executableName, String queryMapKeyComponentName, String serverName, DataFlowBean dataFlowBean) {

        Set<String> columnSet = new HashSet<>();
        Set<String> dbTypeSet = null;
        ACPInputParameterBean acpInputParameterBean = null;

        if (dataFlowBean != null) {
            acpInputParameterBean = dataFlowBean.getAcpInputParameterBean();
            dbTypeSet = acpInputParameterBean.getDbTypeSet();
        }

        String[] outputColumnsArray = null;
        try {
            if (StringUtils.isNotBlank(assignedTotalOutPutColumns)) {
                outputColumnsArray = assignedTotalOutPutColumns.split(",");

            } else if (StringUtils.isNotBlank(returnedTotalInputColumns)) {
                outputColumnsArray = returnedTotalInputColumns.split(",");
            }
            if (outputColumnsArray != null) {
                for (String columName : outputColumnsArray) {
                    if (!org.apache.commons.lang3.StringUtils.isBlank(columName)) {
                        columnSet.add(columName.replace("[", "").replace("]", ""));
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "updateQueryIfContainsStarInIt(-,-)");
        }
        EDbVendor dbVendor = null;
        if (dbTypeSet != null) {
            Iterator<String> dbTypeItr = dbTypeSet.iterator();

            while (dbTypeItr.hasNext()) {
                String dbType = dbTypeItr.next();
                dbVendor = SqlParserUtil.getDBVendorFromStringVendorName(dbType);
                TGSqlParser sqlparser = SqlParserUtil.isQueryParsable(query, dbVendor);
                if (sqlparser != null) {

                    break;
                } else {
                    dbVendor = EDbVendor.dbvmssql;
                }

            }
        }

        try {
            Map<String, Object> argumentsMap = null;
            if (acpInputParameterBean != null) {

                argumentsMap = prepareStarQueryInputsMap(acpInputParameterBean, dataBaseName, serverName, query, dbVendor, columnSet);

                query = DDLQueryGeneratorV1.getDDLAppendedSqlTextForSSIS(argumentsMap);
            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "updateQueryIfContainsStarInIt(-,-)");
        }

        queryMap.put(executableName + Delimiter.ed_ge_Delimiter + queryMapKeyComponentName + "$queryName", query + Delimiter.delimiter + dataBaseName + Delimiter.ed_ge_Delimiter + serverName);
        return query;
    }

    /**
     * this method prepares a hash map containing only non $ keys by iterating
     * the given extended property map
     *
     * @param extendedpropMap
     * @return
     */
    public static Map<String, String> getQuerySet(Map<String, String> extendedpropMap) {
        Map<String, String> propMap = new LinkedHashMap<>();
        try {

            for (Map.Entry<String, String> entry : extendedpropMap.entrySet()) {
                String key = entry.getKey();
                String uKey = key;

                if (!uKey.contains("$")) {
                    continue;
                }
// commented on 14th septeber 2020 by Dinesh to get The Varible Map DeTails Into Extended properties
                String value = entry.getValue();

                propMap.put(uKey, value);
            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getQuerySet(-,-)");

        }
        return propMap;
    }

    /**
     * this method will prepare the inputs map which we are sending to sql
     * parser for star(*) query replacement with ddl command
     *
     * @param acpInputParameterBean
     * @param dataBaseName
     * @param serverName
     * @param query
     * @param dbVendor
     * @param columnSet
     * @return
     */
    public static Map<String, Object> prepareStarQueryInputsMap(ACPInputParameterBean acpInputParameterBean, String dataBaseName, String serverName, String query, EDbVendor dbVendor, Set<String> columnSet) {
        Map<String, Object> argumentsMap = new HashMap<>();
        Map<String, String> ddlCacheMap = new HashMap<>();
        try {
            argumentsMap.put("sqlText", query);
            argumentsMap.put("ddlCacheMap", ddlCacheMap);
            argumentsMap.put("metadatChacheHM", acpInputParameterBean.getMetaDataCacheMap());
            argumentsMap.put("allDBMap", acpInputParameterBean.getAllDBMap());
            argumentsMap.put("defSysName", acpInputParameterBean.getDefaultSystemName());
            argumentsMap.put("defEnvName", acpInputParameterBean.getDefaultEnvrionmentName());
            argumentsMap.put("delimiter", Delimiter.delimiter);
            argumentsMap.put("dbName", dataBaseName);
            argumentsMap.put("servername", serverName);
            argumentsMap.put("jsonFilePath", acpInputParameterBean.getMetadataSyncupPath());
            argumentsMap.put("defSchema", acpInputParameterBean.getDefaultSchema());
            argumentsMap.put("smUtil", acpInputParameterBean.getSystemManagerUtil());
            argumentsMap.put("dbVendor", dbVendor);
            argumentsMap.put("columns", columnSet);
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "prepareStarQueryInputsMap(-,-)");
            argumentsMap = new HashMap<>();
        }
        return argumentsMap;
    }
}
