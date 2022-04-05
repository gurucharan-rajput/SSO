/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

import com.ads.api.beans.mm.MappingSpecificationRow;
import com.erwin.cfx.connectors.json.syncup.SyncupWithServerDBSchamaSysEnvCPT;
import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.beans.SyncUpBean;
import com.erwin.cfx.connectors.ssis.generic.controller.SSISController;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date 28-08-2021
 */
public class SyncUpUtil {

    SyncUpUtil() {

    }

    /**
     * in this method we are preparing inputs Map.this will be used as an input
     * to the SyncUP code
     *
     * @author : Dinesh Arasankala
     * @param syncUpBean
     * @param inputsMap
     * @return
     */
    public static HashMap<String, Object> prapareMetaDataSyncUpInputsMap(SyncUpBean syncUpBean, HashMap<String, Object> inputsMap) {
        try {
            if (syncUpBean != null) {

                ACPInputParameterBean acpInputParameterBean = syncUpBean.getAcpInputParameterBean();
                inputsMap.put("tableName", syncUpBean.getTableName());
                inputsMap.put("json", syncUpBean.getJson());
                inputsMap.put("databasename", syncUpBean.getDatabaseName());
                inputsMap.put("servername", syncUpBean.getServerName());
                inputsMap.put("mapName", syncUpBean.getMapName());
                inputsMap.put("querySchemaName", syncUpBean.getSchemaName());
                inputsMap.put("storeProcColumns", syncUpBean.getColumns());
                inputsMap.put("isFileTypeComponent", syncUpBean.isIsFileTypeComponent());
                inputsMap.put("fileDatabaseType", syncUpBean.getFileDatabaseType());
                inputsMap.put("excellFileName", syncUpBean.getExcellFileName());
                if (acpInputParameterBean != null) {
                    inputsMap.put("jsonFilePath", acpInputParameterBean.getMetadataSyncupPath());
                    inputsMap.put("defSysName", acpInputParameterBean.getDefaultSystemName());
                    inputsMap.put("defEnvName", acpInputParameterBean.getDefaultEnvrionmentName());
                    inputsMap.put("cacheMap", acpInputParameterBean.getMetaDataCacheMap());
                    inputsMap.put("allDBMap", acpInputParameterBean.getAllDBMap());
                    inputsMap.put("defSchema", acpInputParameterBean.getDefaultSchema());
                    inputsMap.put("systemManagerUtil", acpInputParameterBean.getSystemManagerUtil());
                    inputsMap.put("storeProcedureTableFlag", acpInputParameterBean.getStoreProcedureTableCreation());
                    inputsMap.put("fileSystemName", acpInputParameterBean.getFileTypeSystemName());
                    inputsMap.put("fileEnvName_CSV", acpInputParameterBean.getFileTypeEnvironmentNameForCSV());
                    inputsMap.put("fileEnvName_XML", acpInputParameterBean.getFileTypeEnvironmentNameForXML());
                    inputsMap.put("fileEnvName_JSON", acpInputParameterBean.getFileTypeEnvironmentNameForJSON());
                    inputsMap.put("fileEnvName_Excel", acpInputParameterBean.getFileTypeDefaultEnvironmentNameForExcel());
                    inputsMap.put("tableClassCaption", acpInputParameterBean.getTableClassCaption());
                    inputsMap.put("metadataDefSystemCreation", acpInputParameterBean.getMetadataCreatedFlag());
                    inputsMap.put("syncupRuleNumbers", acpInputParameterBean.getSyncupRules());
                    inputsMap.put("isMapSyncupInstaedOfJsonFile", acpInputParameterBean.isMapSyncupInstaedOfJsonFile());
                    inputsMap.put("envType", acpInputParameterBean.getEnvType());
                    ////isJsonEncryption  encryptionAndDecryptionKey
                    inputsMap.put("isJsonEncryption", acpInputParameterBean.getJsonEncryption());
                    inputsMap.put("encryptionAndDecryptionKey", acpInputParameterBean.getEncryptionAndDecryptionKey());

                    if (SSISController.isIspacDtsxFileFlag) {
                        inputsMap.put("ssisIspacName", acpInputParameterBean.getIspacName());
                    }
                    inputsMap.put("ssisPackageName", acpInputParameterBean.getInputFileName());
                }

                inputsMap.put("delimiter", Delimiter.delimiter);
                inputsMap.put("tableDelimeter", Delimiter.tableDelimiter);
                inputsMap.put("storeProcDelimeter", Delimiter.storeProcdelimiter);
                inputsMap.put("fileTypeTableCreated", Delimiter.fileTypeTableCreatedFlag);
                inputsMap.put("oDataDelim", Delimiter.oDataSourceDelimiter);

            }
        } catch (Exception e) {
            MappingManagerUtilAutomation.writeExeceptionLog(e, "prapareMetaDataSyncUpInputsMap(-,-)");
        }
        return inputsMap;

    }

    /**
     * this method creates mapping specifications for store procedure in control
     * flow overview map
     *
     * @author : Dinesh Arasankala
     * @param systemEnvironment
     * @param syncUpBean
     * @param componentName
     * @return MappingSpecificationRow
     */
    public static MappingSpecificationRow returnMapSpecRowForStoreProcedure(String systemEnvironment, SyncUpBean syncUpBean, String componentName) {
        MappingSpecificationRow mappingSpecificationRow = new MappingSpecificationRow();
        String[] systemEnvironmentArray = systemEnvironment.split(Delimiter.delimiter);
        int length = systemEnvironmentArray.length;
        String systemName = "";
        String environmentName = "";
        String tableName = "";
        HashMap userDF = new HashMap<>();
        switch (length) {
            case 3:
                systemName = systemEnvironmentArray[0];
                environmentName = systemEnvironmentArray[1];
                tableName = systemEnvironmentArray[2];
                break;
            case 2:
                systemName = systemEnvironmentArray[0];
                environmentName = systemEnvironmentArray[1];
                break;
            case 1:
                systemName = systemEnvironmentArray[0];
                break;
            default:
                break;
        }

        String userDefined1 = "";
        String userDefined2 = "";
        String userDefined3And4 = "  #  ";

        String userSystemName = "";
        String userEnvironmentName = "";
        String postSyncup = "";
        String storeProcedureName = "";
        String columnName = "#Dummy#";
        String heirarchyEnvName = "";

        if (syncUpBean != null) {
            userSystemName = syncUpBean.getAcpInputParameterBean().getDefaultSystemName();
            userEnvironmentName = syncUpBean.getAcpInputParameterBean().getDefaultEnvrionmentName();
            postSyncup = syncUpBean.getAcpInputParameterBean().getPostSyncUp();
            storeProcedureName = syncUpBean.getTableName();
            userDF = syncUpBean.getAcpInputParameterBean().getUserDF();
            userDefined1 = syncUpBean.getServerName();
            userDefined2 = syncUpBean.getDatabaseName();
            heirarchyEnvName = syncUpBean.getHeirarchyFolderEnvName();

        }

        mappingSpecificationRow.setSourceTableName(componentName);
        mappingSpecificationRow.setSourceSystemName(userSystemName);
        mappingSpecificationRow.setSourceSystemEnvironmentName(heirarchyEnvName);
        mappingSpecificationRow.setSourceColumnName(columnName);

        mappingSpecificationRow.setTargetTableName(storeProcedureName);
        mappingSpecificationRow.setTargetSystemName(systemName);
        mappingSpecificationRow.setTargetSystemEnvironmentName(environmentName);
        mappingSpecificationRow.setTargetColumnName(columnName);
        try {
            if (postSyncup.equals("udf")) {
                if (!userDefined1.equals(userSystemName) && !userDefined1.equals(userEnvironmentName) && userDF.get("srcServer") != null) {
                    mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("srcServer").toString()), userDefined1);
                } else if (userDF.get("srcServer") != null) {
                    userDefined1 = userDefined3And4;
                    mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("srcServer").toString()), userDefined1);
                }
                if (!userDefined2.equals(userEnvironmentName) && !userDefined2.equals(userSystemName) && userDF.get("srcDB") != null) {
                    mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("srcDB").toString()), userDefined2);
                } else if (userDF.get("srcDB") != null) {
                    userDefined2 = userDefined3And4;
                    mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("srcDB").toString()), userDefined2);
                }
                if (userDF.get("tgtServer") != null && userDF.get("tgtDB") != null) {
                    mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("tgtServer").toString()), userDefined3And4);
                    mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("tgtDB").toString()), userDefined3And4);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "returnMapSpecRowForStoreProcedure(-,-)");
        }
        return mappingSpecificationRow;
    }

    /**
     * this method returns store procedure name by replacing all the special
     * characters and delimiters present in it and appending with schema if
     * present
     *
     * @param modifiedExtendedPropMap
     * @param storeProcedureName
     * @param componentName
     * @param query
     * @param defaultSchemaName
     * @return
     */
    public static String returnStroreProcedureName(Map<String, String> modifiedExtendedPropMap, String storeProcedureName, String componentName, String query, String defaultSchemaName) {
        String schemaName = "";
        boolean flag = true;
        try {
            ArrayList<String> targetDetailedTableNameList = SyncupWithServerDBSchamaSysEnvCPT.getTableName(storeProcedureName, defaultSchemaName);
            for (String targetDetailedTableName : targetDetailedTableNameList) {

                int length = targetDetailedTableName.split(Delimiter.delimiter).length;

                if (length >= 4) {
                    schemaName = targetDetailedTableName.split(Delimiter.delimiter)[2];

                    storeProcedureName = targetDetailedTableName.split(Delimiter.delimiter)[3];
                    storeProcedureName = storeProcedureName.replaceAll("[^a-zA-Z0-9 \\p{L}\\._-]", "");
                } else {
                    modifiedExtendedPropMap.put(componentName, query);
                    flag = false;
                    continue;
                }

                if (storeProcedureName.equalsIgnoreCase(Delimiter.storeProcdelimiter)) {
                    modifiedExtendedPropMap.put(componentName, query);
                    flag = false;
                    continue;
                }

                if (StringUtils.isBlank(schemaName)) {
                    storeProcedureName = defaultSchemaName + "." + storeProcedureName;
                    schemaName = defaultSchemaName;
                }

            }
        } catch (Exception e) {
            MappingManagerUtilAutomation.writeExeceptionLog(e, "returnStroreProcedureName(-,-)");
            modifiedExtendedPropMap.put(componentName, query);
            flag = false;
            e.printStackTrace();
        }
        return storeProcedureName + Delimiter.delimiter + schemaName + Delimiter.delimiter + flag;
    }
}
