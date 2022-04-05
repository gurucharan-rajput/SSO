/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.common;

import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.cfx.connectors.json.syncup.SyncupWithServerDBSchamaSysEnvCPT;
import com.erwin.cfx.connectors.sqlparser.v3.MappingCreator;
import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.beans.SyncUpBean;

import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.ExtreamSourceAndExtreamTarget_Util;
import com.erwin.cfx.connectors.ssis.generic.util.MappingManagerUtilAutomation;
import com.erwin.cfx.connectors.ssis.generic.util.SqlParserUtil;
import com.erwin.cfx.connectors.ssis.generic.util.SyncUpUtil;
import com.erwin.sqlparser.DDLQueryGeneratorV1;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 31-08-2021
 *
 */
public class CreateQueryMappings {

    CreateQueryMappings() {

    }

    /**
     * this method creates mapping for queries present in the control flow and
     * data flow
     *
     * @param queryFilePath
     * @param projectId
     * @param subjectId
     * @param dtsxPackageName
     * @param acpInputParameterBean
     * @param controlFlowOrDataflowCheck
     * @param packageLog
     * @param queryMapSet
     * @param hierarchyEnvironementName
     * @return
     */
    public static String createQueryLevelMappings(String queryFilePath, int projectId, int subjectId, String dtsxPackageName, ACPInputParameterBean acpInputParameterBean, String controlFlowOrDataflowCheck, String packageLog, Set<String> queryMapSet, String hierarchyEnvironementName) {

        StringBuilder logBuilder = new StringBuilder();
        logBuilder.append("\n\n*** " + queryFilePath + " ***\n\n");
        String json = "";
        String queryString = "";
        String query = "";
        String componentName = "";
        String dataFlowName = "";
        String defSchema = acpInputParameterBean.getDefaultSchema();
        String userSystemName = acpInputParameterBean.getDefaultSystemName();
        String userEnvironmentName = acpInputParameterBean.getDefaultEnvrionmentName();
        MappingCreator mappingCreator = null;
        String systemName = userSystemName;
        String environmentName = userEnvironmentName;
        ArrayList<MappingSpecificationRow> mapspecrows = new ArrayList<>();
        String postSyncUp = acpInputParameterBean.getPostSyncUp();
        HashMap userDF = acpInputParameterBean.getUserDF();
        Map ddlCacheMap = acpInputParameterBean.getDdlCacheMap();
        HashMap<String, String> allDBMap = acpInputParameterBean.getAllDBMap();
        String metadataJsonPath = acpInputParameterBean.getMetadataSyncupPath();
        SystemManagerUtil systemManagerUtil = acpInputParameterBean.getSystemManagerUtil();
        HashMap<String, String> openQueryMap = acpInputParameterBean.getOpenQueryHashMap();
        SyncUpBean syncUpBean = new SyncUpBean();
        KeyValueUtil keyValueUtil = acpInputParameterBean.getKeyValueUtil();
        String actualQuery = "";
        try {
            long startTime = System.currentTimeMillis();
            File fileLocation = new File(queryFilePath + ".sql");

            if (fileLocation.canRead()) {
                queryString = FileUtils.readFileToString(fileLocation, "UTF-8");
            }

            queryFilePath = FilenameUtils.normalizeNoEndSeparator(queryFilePath, true);

            if (queryFilePath.contains("/")) {
                componentName = queryFilePath.substring(queryFilePath.lastIndexOf("/") + 1, queryFilePath.length());
            } else {
                componentName = queryFilePath;
            }

            try {
                if (componentName.contains("_ED_GE")) {
                    String componentNameSpilt[] = componentName.split("_ED_GE");
                    if (componentNameSpilt.length >= 1) {
                        componentName = componentNameSpilt[1];
                        dataFlowName = componentNameSpilt[0];
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            String systemAndEnvironmentName = "";
            try {

                if (queryString.contains(Delimiter.delimiter)) {

                    int length = queryString.split(Delimiter.delimiter).length;
                    if (length >= 2) {
                        query = queryString.split(Delimiter.delimiter)[0];
                        systemAndEnvironmentName = queryString.split(Delimiter.delimiter)[1];
                    }
                    if (systemAndEnvironmentName.contains("_ED_GE")) {
                        length = systemAndEnvironmentName.split("_ED_GE").length;
                        if (length == 2) {
                            systemName = systemAndEnvironmentName.split("_ED_GE")[1];
                            environmentName = systemAndEnvironmentName.split("_ED_GE")[0];
                        } else if (length == 1) {
                            environmentName = systemAndEnvironmentName.split("_ED_GE")[0];
                        }

                    }

                } else {
                    query = queryString;
                }

                if (systemName.contains(";")) {
                    systemName = systemName.replace(";", "");
                }

            } catch (Exception e) {
                e.printStackTrace();
                systemName = userSystemName;
                environmentName = userEnvironmentName;
            }

            try {
                String queryContent = getQueryContent(query);

                String filePathResult = "";
                String storeProcName = "";
                try {

                    if (queryContent.contains(Delimiter.delimiter)) {

                        int queryContentLength = queryContent.split(Delimiter.delimiter).length;

                        if (queryContentLength == 3) {
                            query = queryContent.split(Delimiter.delimiter)[0];
                            filePathResult = queryContent.split(Delimiter.delimiter)[1];
                            storeProcName = queryContent.split(Delimiter.delimiter)[2];
                        } else if (queryContentLength == 2) {
                            query = queryContent.split(Delimiter.delimiter)[0];
                            filePathResult = queryContent.split(Delimiter.delimiter)[1];
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (controlFlowOrDataflowCheck.equals("DataFlow")) {
                    componentName = dataFlowName + "__PKG__" + dtsxPackageName + "__" + componentName;

                }
                componentName = componentName.replaceAll("[^a-zA-Z0-9 \\p{L}_-]", "_");
                if (StringUtils.isNotBlank(componentName) && componentName.length() >= 300) {
                    componentName = componentName.substring(0, 295) + "...";
                }

                if (componentName.trim().equalsIgnoreCase("Get Sin Historial")) {
                    query = removeUnParsedContentFromSqlData(query);
                }
                Set<String> dbTypeSet = acpInputParameterBean.getDbTypeSet();
                String defaultDbType = acpInputParameterBean.getDefaultDatabaseType();
                Iterator<String> dbTypeItr = dbTypeSet.iterator();
                String passedDbType = "";
                EDbVendor dbVendor = null;
                while (dbTypeItr.hasNext()) {
                    String dbType = dbTypeItr.next();
                    dbVendor = SqlParserUtil.getDBVendorFromStringVendorName(dbType);
                    TGSqlParser sqlparser = SqlParserUtil.isQueryParsable(query, dbVendor);
                    if (sqlparser != null) {
                        passedDbType = dbType;
                        break;
                    } else {
                        dbVendor = EDbVendor.dbvmssql;
                        passedDbType = defaultDbType;
                    }

                }

                String databaseName = environmentName.trim();
                String serverName = systemName.trim();

                if (databaseName.equalsIgnoreCase(userEnvironmentName)) {
                    databaseName = "";
                }

                mappingCreator = new MappingCreator();

                try {

                    syncUpBean.setDatabaseName(databaseName);
                    syncUpBean.setServerName(serverName);
                    syncUpBean.setAcpInputParameterBean(acpInputParameterBean);

                    HashMap<String, Object> argumentsMap = new HashMap<>();

                    argumentsMap.put("ddlCacheMap", ddlCacheMap);
                    argumentsMap.put("metadatChacheHM", acpInputParameterBean.getMetaDataCacheMap());
                    argumentsMap.put("allDBMap", allDBMap);
                    argumentsMap.put("defSysName", userSystemName);
                    argumentsMap.put("defEnvName", userEnvironmentName);
                    argumentsMap.put("delimiter", Delimiter.delimiter);
                    argumentsMap.put("dbName", databaseName);
                    argumentsMap.put("servername", serverName);
                    argumentsMap.put("jsonFilePath", metadataJsonPath);
                    argumentsMap.put("defSchema", defSchema);
                    argumentsMap.put("dbVendor", dbVendor);
                    argumentsMap.put("smUtil", systemManagerUtil);
                    argumentsMap.put("isMapSyncupInstaedOfJsonFile", acpInputParameterBean.isMapSyncupInstaedOfJsonFile());
                    json = mappingCreator.getMappingObjectToJsonForSSIS(query, userSystemName, userEnvironmentName, projectId, passedDbType, componentName, subjectId, defSchema,
                            serverName, databaseName, postSyncUp, userDF, allDBMap, openQueryMap, argumentsMap);

                    syncUpBean.setJson(json);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if ("".equals(json) || json == null) {
                    String appendResult = "";
                    if (!filePathResult.equalsIgnoreCase("null")) {
                        appendResult = dtsxPackageName + ".dtsx" + "==>" + "Query is not able to parse  : " + "==>" + componentName + " , filePath :- " + filePathResult + "\n";
                    } else if (StringUtils.isBlank(storeProcName)) {
                        appendResult = dtsxPackageName + ".dtsx" + "==>" + "Query is not able to parse  : " + "==>" + componentName + "\n";

                    }

                    logBuilder.append("Empty Json for query\n").append(appendResult);
                    return logBuilder.toString();
                }

//                
                if ("".equals(json) || json == null) {
                    logBuilder.append("Empty Json for query");
                    return logBuilder.toString();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            Mapping mapObj = new Mapping();

            try {
                if (StringUtils.isNotBlank(json)) {
                    queryMapSet.add(componentName);

                    HashMap inputsMap = new HashMap();
                    inputsMap = SyncUpUtil.prapareMetaDataSyncUpInputsMap(syncUpBean, inputsMap);
                    inputsMap.put("intermediateCompSet", MappingCreator.intermediateComponents);
                    inputsMap.put("openQueryDelimiter", Delimiter.openQueryDelimiter);
//                    if (acpInputParameterBean.isIsVolumeSyncUp()) {
//                        mapspecrows = com.erwin.cfx.connectors.json.syncup.v1.SyncupWithServerDBSchamaSysEnvCPT.setMetaDataSpec(inputsMap);
//                    } else {
                    mapspecrows = SyncupWithServerDBSchamaSysEnvCPT.setMetaDataSpec(inputsMap);
//                    }

                }
            } catch (Exception e) {

            }

            if (mapspecrows.isEmpty()) {
                logBuilder.append("Empty Specifications Are Thier In The query");
                return logBuilder.toString();
            }

            hierarchyEnvironementName = CreateDataFlowMappings.returnHirerchalEnvName(hierarchyEnvironementName, dtsxPackageName);

            removeSpecIfTargetTableIsEmprty(mapspecrows, hierarchyEnvironementName, MappingCreator.intermediateComponents, controlFlowOrDataflowCheck);
            Set<String> extremeTargetTableSet = ExtreamSourceAndExtreamTarget_Util.getExtreamTargetTablesSet(mapspecrows, hierarchyEnvironementName, MappingCreator.intermediateComponents);
            if (controlFlowOrDataflowCheck.equals("DataFlow")) {

                for (String extreamTargetTable : extremeTargetTableSet) {
                    if (!extreamTargetTable.equals("")) {
                        changeTheExtremeTargetSystemandEnvandComponentForDataflow(mapspecrows, componentName, hierarchyEnvironementName, extreamTargetTable, userSystemName);
                    }

                }
            } else {
                changeTheExtremeTargetEnvNameForControlFLow(mapspecrows, hierarchyEnvironementName, extremeTargetTableSet);
            }

            componentName = componentName.replaceAll("[^a-zA-Z0-9 \\p{L}_-]", "_");

            mapObj.setMappingSpecifications(mapspecrows);
            mapObj.setMappingName(componentName);
            mapObj.setProjectId(projectId);
            mapObj.setSubjectId(subjectId);
            if ("true".equalsIgnoreCase("true")) {
                mapObj.setUpdateSourceMetadata(true);
                mapObj.setUpdateTargetMetadata(true);
            }

            if (StringUtils.isNotBlank(actualQuery)) {
                mapObj.setSourceExtractQuery(actualQuery);
            } else {
                mapObj.setSourceExtractQuery(query);
            }

            mapObj.setTestingNotes("test---");

            int mappingID = MappingManagerUtilAutomation.createMappings(subjectId, componentName, acpInputParameterBean, mapspecrows, mapObj, packageLog, logBuilder);

            if (mappingID > 0) {
                LinkedHashMap<String, HashSet<String>> keyValuesDeailsMap = MappingCreator.keyValuesDeailsMap;
                String status = MappingCreator.addKeyValues(mappingID, keyValuesDeailsMap, keyValueUtil);
                String reqStatus = " Extended Properties Status ===> " + status + "\n\n";
                logBuilder.append(reqStatus);
            }
            long endTime = System.currentTimeMillis();
            long milliseconds = endTime - startTime;
            long minutes = TimeUnit.MILLISECONDS.toMinutes(milliseconds);

            String formatTme = String.format("%d Milliseconds = %d minutes\n", milliseconds, minutes);

            System.out.print("Mapping Name---" + componentName + " ");
            System.out.println("Mapping Time---" + formatTme);
        } catch (Exception e) {
            logBuilder.append(e).append("\n\n");
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "createQueryLevelMappings(-,-)");
            return logBuilder.toString();

        }

        return logBuilder.toString();

    }

    /**
     * this method returns dynamic query from the query content
     *
     * @param query
     * @return
     */
    public static String getQueryContent(String query) {
        String sqlFile = null;
        String key = "";
        try {

            if (query.toUpperCase().contains("SET @SQL") && query.toUpperCase().contains("EXEC")) {
                query = extarctDynamicQueryFromSqlFile(query);
            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getQueryContent(-,-)");
        }

        String returnData = query + Delimiter.delimiter + sqlFile + Delimiter.delimiter + key;
        return returnData;
    }

    /**
     * this method extract dynamic query from the query content
     *
     * @param query
     * @return
     */
    public static String extarctDynamicQueryFromSqlFile(String query) {

        try {

            if (query.toUpperCase().contains("SET @SQL") && query.toUpperCase().contains("EXEC")) {

                String[] dynamicQuery = query.toUpperCase().split("SET @SQL = '");

                if (dynamicQuery.length >= 1) {

                    query = dynamicQuery[1];

                    if (query.toUpperCase().contains("SET")) {

                        dynamicQuery = query.toUpperCase().split("SET");

                        if (dynamicQuery.length >= 1) {
                            query = dynamicQuery[0];
                        }

                        dynamicQuery = query.split("\n");

                        query = "";
                        for (String queryLine : dynamicQuery) {
                            if (!queryLine.toUpperCase().trim().startsWith("EXEC")) {
                                query = query + queryLine + "\n";
                            }
                        }

                    }

                }

                query = query.replaceAll("'", "");

            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "extarctDynamicQueryFromSqlFile(-,-)");

        }
        return query;
    }

    /**
     * in this method,updating the query with the DDL command if it contains (*)
     * in it
     *
     * @param query
     *
     * @param dataBaseName
     * @param serverName
     * @param dbVendor
     * @param acpInputParameterBean
     *
     * @return query with DDL command
     */
    public static String updateQueryIfContainsStarInIt(String query, String dataBaseName, String serverName, EDbVendor dbVendor, ACPInputParameterBean acpInputParameterBean) {

        try {
            Map<String, Object> argumentsMap = null;

            Set<String> columnSet = new HashSet<>();
            argumentsMap = SqlParserUtil.prepareStarQueryInputsMap(acpInputParameterBean, dataBaseName, serverName, query, dbVendor, columnSet);

            if (argumentsMap != null) {
                query = DDLQueryGeneratorV1.getDDLAppendedSqlTextForSSIS(argumentsMap);
            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "updateQueryIfContainsStarInIt(-,-)");

        }

        return query;
    }

    /**
     * this method removes the unparsable data from the query
     *
     * @param queryContent
     * @return
     */
    public static String removeUnParsedContentFromSqlData(String queryContent) {

        String modifiedQueryData = "";
        try {
            String spiltQueryContent[] = queryContent.split("\n");
            for (String spiltData : spiltQueryContent) {
                if (spiltData.toUpperCase().contains("PARTITION BY") && spiltData.toUpperCase().contains("UNBOUNDED PRECEDING") && spiltData.toUpperCase().contains("CURRENT ROW")) {

                    spiltData = spiltData.replace("DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", "");
                    modifiedQueryData = modifiedQueryData + spiltData + "\n";

                } else {
                    modifiedQueryData = modifiedQueryData + spiltData + "\n";
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "removeUnParsedContentFromSqlData(-,-)");
        }
        return modifiedQueryData;
    }

    /**
     * this method updates the extreme target environment name with map name for
     * data flow query level mappings
     *
     * @param mappingSpecifications
     * @param componentName
     * @param dataflowName
     * @param extremetgtTable
     * @param defaultSystemName
     */
    public static void changeTheExtremeTargetSystemandEnvandComponentForDataflow(ArrayList<MappingSpecificationRow> mappingSpecifications, String componentName, String dataflowName, String extremetgtTable, String defaultSystemName) {
        try {
            Iterator<MappingSpecificationRow> iter = mappingSpecifications.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                String tgtTabName = row.getTargetTableName();
                if (tgtTabName.equalsIgnoreCase(extremetgtTable)) {
                    row.setTargetTableName(componentName);
                    row.setTargetSystemEnvironmentName(dataflowName);
                    row.setTargetSystemName(defaultSystemName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "changeTheExtremeTargetSystemandEnvandComponent(-,-)");

        }
    }

    /**
     * this method updates the extreme target environment name with map name for
     * data flow query level mappings
     *
     * @param mappingSpecifications
     * @param heirarchyFolderName
     * @param extremetgtTableSet
     *
     *
     * @param extremetgtTable
     *
     */
    public static void changeTheExtremeTargetEnvNameForControlFLow(ArrayList<MappingSpecificationRow> mappingSpecifications, String heirarchyFolderName, Set<String> extremetgtTableSet) {
        try {
            Iterator<MappingSpecificationRow> iter = mappingSpecifications.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                String tgtTabName = row.getTargetTableName();

                if (extremetgtTableSet.contains(tgtTabName.toUpperCase())) {

                    row.setTargetSystemEnvironmentName(heirarchyFolderName);

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "changeTheExtremeTargetEnvNameForControlFLow(-,-)");

        }
    }

    /**
     * this method removes mapping specification if target table is empty
     *
     * @param mappingspec
     */
    public static void removeSpecIfTargetTableIsEmprty(ArrayList<MappingSpecificationRow> mappingspec, String hierarchyEnvironementName, Set<String> intermediateSet, String controlFlowOrDataflowCheck) {

        try {
            Iterator<MappingSpecificationRow> iter = mappingspec.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
//                if (controlFlowOrDataflowCheck.equals("DataFlow")) {

                    String sourceEnvName = row.getSourceSystemEnvironmentName();
                    String srcTbl = row.getSourceTableName();
                    String trtTbl = row.getTargetTableName();

                    String targetEnvName = row.getTargetSystemEnvironmentName();
                    String srcTab[] = srcTbl.split("\n");
                    for (String sourceTable : srcTab) {

                        ExtreamSourceAndExtreamTarget_Util.updateInterMediateEnvironmentName(row, sourceTable, intermediateSet, hierarchyEnvironementName, sourceEnvName, true);

                    }
                    String trtTab[] = trtTbl.split("\n");
                    for (String targetTable : trtTab) {

                        ExtreamSourceAndExtreamTarget_Util.updateInterMediateEnvironmentName(row, targetTable, intermediateSet, hierarchyEnvironementName, targetEnvName, false);

                    }
//                }
                if (row.getTargetTableName().equals("") || row.getTargetTableName() == null) {

                    iter.remove();
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "removeSpecIfTargetTableIsEmprty(-,-)");
        }

    }
}
