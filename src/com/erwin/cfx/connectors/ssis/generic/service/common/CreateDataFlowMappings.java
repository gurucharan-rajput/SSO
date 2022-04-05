/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.common;

import com.ads.api.beans.common.AuditHistory;
import com.ads.api.beans.common.Node;
import com.ads.api.beans.kv.KeyValue;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.util.KeyValueUtil;
import com.erwin.cfx.connectors.json.syncup.SyncupWithServerDBSchamaSysEnvCPT;
import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.beans.SyncUpBean;
import com.erwin.cfx.connectors.ssis.generic.controller.SSISController;
import com.erwin.cfx.connectors.ssis.generic.service.ssis2008.IterateInputAndOutPutColumns2008;
import com.erwin.cfx.connectors.ssis.generic.service.ssis2008.PrepareDataFlowLineage2008;
import com.erwin.cfx.connectors.ssis.generic.service.ssis2014.IterateInputAndOutPutColumns2014;
import com.erwin.cfx.connectors.ssis.generic.service.ssis2014.PrepareDataFlowLineage2014;
import com.erwin.cfx.connectors.ssis.generic.util.BoxingAndUnBoxingWrapper;
import com.erwin.cfx.connectors.ssis.generic.util.Constants;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.ExtreamSourceAndExtreamTarget_Util;
import com.erwin.cfx.connectors.ssis.generic.util.MappingManagerUtilAutomation;
import com.erwin.cfx.connectors.ssis.generic.util.SyncUpUtil;
import com.erwin.cfx.connectors.ssis.generic.util.UserDefinedField;
import com.icc.util.RequestStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 30-08-2021
 */
public class CreateDataFlowMappings {

    CreateDataFlowMappings() {

    }

    public static Set<String> dataFlowStoreProcCallSet = new HashSet<>();
    public static Map<String, String> conditionalSplitMapForExtendedProperties = new HashMap<>();
    public static HashMap<String, String> dataFlowSpecificationMap = new HashMap<>();

    /**
     * this methods creates mapping for the data flow components
     *
     * @param dataFlowLevelLineageMap
     * @param acpInputParameter
     * @param projectId
     * @param subjectId
     * @param dataFlowSet
     * @param packageLog
     * @param hierarchyEnvironementName
     */
    public static String createDataFlowMappings(Map<String, List<HashMap<String, String>>> dataFlowLevelLineageMap, ACPInputParameterBean acpInputParameter, int projectId, int subjectId, Set<Map<String, String>> dataFlowSet, String packageLog, String hierarchyEnvironementName) {

        if (acpInputParameter == null) {
            acpInputParameter = new ACPInputParameterBean();
        }
        StringBuilder logBuilder = new StringBuilder();
        String hierarchyEnvName = "";

        IterateInputAndOutPutColumns2014 inputAndOutPutColumns2014 = new IterateInputAndOutPutColumns2014();

        IterateInputAndOutPutColumns2008 inputAndOutPutColumns2008 = new IterateInputAndOutPutColumns2008();

        String userSystemName = acpInputParameter.getDefaultSystemName();
        String userEnvironmentName = acpInputParameter.getDefaultEnvrionmentName();
        String defSchema = acpInputParameter.getDefaultSchema();
        String postSyncup = acpInputParameter.getPostSyncUp();
        Map<String, String> userDF = acpInputParameter.getUserDF();
        KeyValueUtil keyValueUtil = acpInputParameter.getKeyValueUtil();
        List<String> fileExtensionList = acpInputParameter.getFileExtensionList();
        String packageName = acpInputParameter.getInputFileName();

        if (dataFlowLevelLineageMap != null) {
            for (Map.Entry<String, List<HashMap<String, String>>> entrySet : dataFlowLevelLineageMap.entrySet()) {
                try {
                    long startTime = System.currentTimeMillis();
                    String mapName = entrySet.getKey();
                    if (StringUtils.isNotBlank(mapName) && mapName.length() >= 300) {
                        mapName = mapName.substring(0, 295) + "...";
                    }

                    hierarchyEnvName = "";
                    hierarchyEnvName = hierarchyEnvironementName;
                    conditionalSplitMapForExtendedProperties = new HashMap<>();
                    dataFlowStoreProcCallSet = new HashSet<>();
                    dataFlowSpecificationMap = new HashMap<>();
                    ArrayList<MappingSpecificationRow> mappingSpecifications = new ArrayList<>();
                    Mapping mapping = new Mapping();
                    Set<String> removeDuplicates = new LinkedHashSet<>();
                    List<HashMap<String, String>> list = entrySet.getValue();

                    HashMap<String, String> componentLineageMap = list.get(0);
                    HashMap<String, String> extendedPropertiesMap = list.get(1);
                    Map<String, String> extendedPropertiesDup = new HashMap<>();
                    extendedPropertiesMap = getTheExecuteCallSFromTheExtendedPropertiesMap(extendedPropertiesMap, dataFlowStoreProcCallSet);

                    String sourceTableClass = "";
                    String targetTableClass = "";
                    try {
                        for (Map.Entry<String, String> componentLineageEntrySet : componentLineageMap.entrySet()) {
                            String target = componentLineageEntrySet.getKey();
                            String multiSource = componentLineageEntrySet.getValue();
                            targetTableClass = target.split(Delimiter.delimiter)[6];
                            String userdefined1 = "";
                            String userdefined2 = "";
                            String userdefined3 = "";
                            String userdefined4 = "";
                            boolean isFileComponentFlag = false;

                            for (int i = 0; i < multiSource.split("$$").length; i++) {
                                MappingSpecificationRow mapSpecRow = new MappingSpecificationRow();
                                String srcEnvironmentName = userEnvironmentName;
                                String srcSystemName = userSystemName;
                                String tgtEnvironmentName = userEnvironmentName;
                                String tgtSystemName = userSystemName;
                                boolean sourceColumnSpecialCharatcters = false;
                                boolean targetColumnSpecialCharatcters = false;

                                String source = multiSource.split("$$")[i];
                                for (int idx = 0; idx < source.split("%%").length; idx++) {
                                    String eachsource = source.split("%%")[idx];
                                    isFileComponentFlag = false;
                                    userdefined1 = "";
                                    userdefined2 = "";
                                    userdefined3 = "";
                                    userdefined4 = "";
                                    sourceColumnSpecialCharatcters = false;
                                    targetColumnSpecialCharatcters = false;
                                    String srcFileExtension = "";
                                    String[] eachSourceArray = eachsource.split(Delimiter.delimiter);
                                    int sourceLength = eachSourceArray.length;

                                    if (sourceLength <= 0) {
                                        continue;
                                    }
                                    if (sourceLength >= 7) {
                                        sourceTableClass = eachSourceArray[6];
                                    }

                                    String srcTableName = "";
                                    String srcComponentName = "";

                                    String srcColName = "";
                                    if (sourceLength >= 2) {
                                        srcTableName = eachSourceArray[0];
                                        if (srcTableName.contains("\n")) {
                                            srcTableName = srcTableName.split("\n")[0];
                                        }
                                        srcTableName = srcTableName.replace("(", "").replace(")", "");
                                        srcColName = eachSourceArray[1].trim();
                                    }
                                    String[] srcTableNameArray = srcTableName.split(Delimiter.ed_ge_Delimiter);
                                    int sourceTablelength = srcTableNameArray.length;
                                    switch (sourceTablelength) {
                                        case 5:
                                            srcTableName = srcTableNameArray[0];
                                            userdefined1 = srcTableNameArray[2];
                                            userdefined2 = srcTableNameArray[1];
                                            srcComponentName = srcTableNameArray[4];
                                            break;
                                        case 4:
                                            srcTableName = srcTableNameArray[0];
                                            userdefined1 = srcTableNameArray[2];
                                            userdefined2 = srcTableNameArray[1];
                                            break;
                                        case 3:
                                            srcTableName = srcTableNameArray[0];
                                            srcComponentName = srcTableNameArray[2];
                                            break;
                                        case 2:
                                            srcTableName = srcTableNameArray[0];

                                            break;
                                        default:
                                            break;
                                    }

                                    if (StringUtils.isNotBlank(srcTableName) && (srcTableName.contains(Delimiter.tableDelimiter) || srcTableName.contains(Delimiter.storeProcdelimiter))) {
                                        String systemEnvironment = getSystemAndEnvFromDFTableName(srcTableName, mapName, defSchema, acpInputParameter, userdefined2, userdefined1, srcComponentName, srcColName);
                                        String[] systemEnvironmentArray = systemEnvironment.split(Delimiter.systemAndDBSeparator);
                                        int length = systemEnvironmentArray.length;
                                        if (length == 5) {
                                            systemEnvironment = systemEnvironmentArray[0];
                                            userdefined1 = systemEnvironmentArray[1];
                                            userdefined2 = systemEnvironmentArray[2];
                                            srcFileExtension = systemEnvironmentArray[3];
                                        }

                                        String[] systemAndEnv = systemEnvironment.split(Delimiter.delimiter);
                                        int sysANdEnvLength = systemAndEnv.length;
                                        String tempSourceTable = "";
                                        switch (sysANdEnvLength) {
                                            case 3:
                                                srcSystemName = systemAndEnv[0];
                                                srcEnvironmentName = systemAndEnv[1];
                                                tempSourceTable = systemAndEnv[2];
                                                break;
                                            case 2:
                                                srcSystemName = systemAndEnv[0];
                                                srcEnvironmentName = systemAndEnv[1];
                                                break;
                                            case 1:
                                                srcSystemName = systemAndEnv[0];
                                                srcEnvironmentName = userEnvironmentName;
                                                break;
                                            default:
                                                srcSystemName = userSystemName;
                                                srcEnvironmentName = userEnvironmentName;
                                                break;
                                        }
                                        if (tempSourceTable.contains(Delimiter.fileTypeTableCreatedFlag)) {
                                            tempSourceTable = tempSourceTable.split(Delimiter.fileTypeTableCreatedFlag)[0];
                                            sourceColumnSpecialCharatcters = true;
                                        }
                                        if (srcTableName.contains(Delimiter.tableDelimiter)) {
                                            srcTableName = tempSourceTable + Delimiter.tableDelimiter;
                                        } else if (srcTableName.contains(Delimiter.storeProcdelimiter)) {
                                            srcTableName = tempSourceTable + Delimiter.storeProcdelimiter;
                                        }
                                    }

                                    if (fileExtensionList.contains(srcFileExtension.toLowerCase())) {
                                        isFileComponentFlag = true;
                                    }
                                    if (isFileComponentFlag) {
                                        srcTableName = srcTableName.replace(defSchema + ".", "");
                                    }
                                    if (sourceTableClass.equalsIgnoreCase("Microsoft.DataConvert")) {
//                         
                                        Map<String, String> dataConversionColumnMap = new HashMap<>();
                                        if (SSISController.is2014) {
                                            dataConversionColumnMap = inputAndOutPutColumns2014.dataConversionColumnMap;

                                        } else {
                                            dataConversionColumnMap = inputAndOutPutColumns2008.dataConversionColumnMap;
                                        }
                                        srcColName = returnSourceColumnName(dataConversionColumnMap, mapName, srcColName, srcTableName);
                                    }
//                                    if (sourceColumnSpecialCharatcters) {
                                    if (isFileComponentFlag) {
                                        srcColName = srcColName.replaceAll("[^a-zA-Z0-9 \\p{L}\\.$_-]", "_");
                                    }

                                    mapSpecRow.setSourceTableName(srcTableName);
                                    mapSpecRow.setSourceColumnName(srcColName);
                                    mapSpecRow.setSourceTableClass(sourceTableClass);
                                    if (sourceLength > 2) {
                                        mapSpecRow.setSourceColumnDatatype(eachSourceArray[2]);
                                    }

                                    int srcColumnLength = 0;
                                    int srcColumnScale = 0;
                                    int srcColumnPrecision = 0;

                                    if (sourceLength >= 4 && !Constants.NOT_DEFINED.equals(eachSourceArray[3]) && !"".equals(eachSourceArray[3])) {
                                        srcColumnLength = BoxingAndUnBoxingWrapper.convertStringToInteger(eachSourceArray[3]);

                                    }

                                    if (sourceLength >= 5 && !Constants.NOT_DEFINED.equals(eachSourceArray[4]) && !"".equals(eachSourceArray[4])) {

                                        srcColumnPrecision = BoxingAndUnBoxingWrapper.convertStringToInteger(eachSourceArray[4]);
                                    }

                                    if (sourceLength >= 6 && !Constants.NOT_DEFINED.equals(eachSourceArray[5]) && !"".equals(eachSourceArray[5])) {

                                        srcColumnScale = BoxingAndUnBoxingWrapper.convertStringToInteger(eachSourceArray[5]);
                                    }

                                    // done the change due to change in the method parameter datatype in EMM10.1
                                    String stringSourceColumnLength = srcColumnLength + "";
                                    String stringSourceColumnPrecision = srcColumnPrecision + "";
                                    String stringSourceColumnScale = srcColumnScale + "";

                                    try {
                                        if (postSyncup.equals("udf")) {

                                            if (StringUtils.isNotBlank(userdefined1) && !userdefined1.equals(userSystemName) && !userdefined1.equals(userEnvironmentName) && userDF.get("srcServer") != null && !userDF.get("srcServer").equals("")) {
                                                mapSpecRow = UserDefinedField.setData(mapSpecRow, Integer.parseInt(userDF.get("srcServer").toString()), userdefined1);
                                            } else if (userDF.get("srcServer") != null && !userDF.get("srcServer").equals("")) {
                                                userdefined1 = "  @  ";
                                                mapSpecRow = UserDefinedField.setData(mapSpecRow, Integer.parseInt(userDF.get("srcServer").toString()), userdefined1);
                                            }
                                            if (StringUtils.isNotBlank(userdefined2) && !userdefined2.equals(userEnvironmentName) && !userdefined2.equals(userSystemName) && userDF.get("srcDB") != null && !userDF.get("srcDB").equals("")) {
                                                mapSpecRow = UserDefinedField.setData(mapSpecRow, Integer.parseInt(userDF.get("srcDB").toString()), userdefined2);
                                            } else if (userDF.get("srcDB") != null && !userDF.get("srcDB").equals("")) {
                                                userdefined2 = "  @  ";
                                                mapSpecRow = UserDefinedField.setData(mapSpecRow, Integer.parseInt(userDF.get("srcDB").toString()), userdefined2);
                                            }
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    mapSpecRow.setSourceColumnLength(stringSourceColumnLength);
                                    mapSpecRow.setSourceColumnPrecision(stringSourceColumnPrecision);
                                    mapSpecRow.setSourceColumnScale(stringSourceColumnScale);
                                    String srcBusinessRule = "";

                                    if (sourceLength > 8 && !eachSourceArray[8].contains("NOT_DEFINED")) {
                                        srcBusinessRule = eachSourceArray[8];

                                    }

                                    try {

                                        Map<String, String> conditionalSplitBusinessRules = new HashMap<>();
                                        if (SSISController.is2014) {
                                            if (inputAndOutPutColumns2014.conditionalSplitBusinessRuleMap.get(mapName + "~" + srcTableName) != null) {
                                                conditionalSplitBusinessRules = inputAndOutPutColumns2014.conditionalSplitBusinessRuleMap.get(mapName + "~" + srcTableName);
                                            }
                                        } else {
                                            if (inputAndOutPutColumns2008.conditionalSplitBusinessRuleMap.get(mapName + "~" + srcTableName) != null) {
                                                conditionalSplitBusinessRules = inputAndOutPutColumns2008.conditionalSplitBusinessRuleMap.get(mapName + "~" + srcTableName);
                                            }
                                        }
                                        for (Map.Entry<String, String> entry : conditionalSplitBusinessRules.entrySet()) {
                                            String outputName = entry.getKey();
                                            String businessRule = entry.getValue();
                                            String key = srcTableName + "_" + outputName;
                                            conditionalSplitMapForExtendedProperties.put(key, businessRule);
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    String mergeSourceKey = mapName + "~" + srcTableName;

                                    if (SSISController.is2014 && PrepareDataFlowLineage2014.mergeComponentMapForExtendedProperties.get(mergeSourceKey) != null) {
                                        conditionalSplitMapForExtendedProperties.put(srcTableName, PrepareDataFlowLineage2014.mergeComponentMapForExtendedProperties.get(mergeSourceKey));
                                    }

                                    if (!StringUtils.isBlank(srcBusinessRule)) {
                                        mapSpecRow.setBusinessRule(srcBusinessRule);
                                    }

                                    isFileComponentFlag = false;
                                    //target Table Data Starts
                                    String tgtTableName = "";
                                    String tgtComponentName = "";
                                    String tgtColumnName = "";
                                    String tgtFileExtension = "";
                                    String[] targetArray = target.split(Delimiter.delimiter);
                                    int targetLength = targetArray.length;

                                    if (targetLength >= 2) {
                                        tgtTableName = targetArray[0];
                                        if (tgtTableName.contains("\n")) {
                                            tgtTableName = tgtTableName.split("\n")[0];
                                        }
                                        tgtTableName = tgtTableName.replace("(", "").replace(")", "");

                                        tgtColumnName = targetArray[1];
                                    }

                                    String[] tgtTableNameArray = tgtTableName.split(Delimiter.ed_ge_Delimiter);
                                    int targetTablelength = tgtTableNameArray.length;
                                    switch (targetTablelength) {
                                        case 5:
                                            tgtTableName = tgtTableNameArray[0];
                                            userdefined3 = tgtTableNameArray[2];
                                            userdefined4 = tgtTableNameArray[1];
                                            tgtComponentName = tgtTableNameArray[4];
                                            break;
                                        case 4:
                                            tgtTableName = tgtTableNameArray[0];
                                            userdefined3 = tgtTableNameArray[2];
                                            userdefined4 = tgtTableNameArray[1];
                                            break;
                                        case 3:
                                            tgtTableName = tgtTableNameArray[0];
                                            tgtComponentName = tgtTableNameArray[2];
                                            break;
                                        case 2:
                                            tgtTableName = tgtTableNameArray[0];
                                            break;
                                        default:
                                            break;
                                    }
                                    if (StringUtils.isNotBlank(tgtTableName) && (tgtTableName.contains(Delimiter.tableDelimiter) || tgtTableName.contains(Delimiter.storeProcdelimiter))) {
                                        String tgtSystemEnvironment = getSystemAndEnvFromDFTableName(tgtTableName, mapName, defSchema, acpInputParameter, userdefined4, userdefined3, tgtComponentName, tgtColumnName);

                                        String[] tgtSystemEnvironmentArray = tgtSystemEnvironment.split(Delimiter.systemAndDBSeparator);
                                        int tgtLength = tgtSystemEnvironmentArray.length;
                                        if (tgtLength == 5) {
                                            tgtSystemEnvironment = tgtSystemEnvironmentArray[0];
                                            userdefined3 = tgtSystemEnvironmentArray[1];
                                            userdefined4 = tgtSystemEnvironmentArray[2];
                                            tgtFileExtension = tgtSystemEnvironmentArray[3];
                                        }
                                        String tempTargetTable = "";
                                        String[] tgtSystemAndEnv = tgtSystemEnvironment.split(Delimiter.delimiter);
                                        int tgtSysANdEnvLength = tgtSystemAndEnv.length;

                                        switch (tgtSysANdEnvLength) {
                                            case 3:
                                                tgtSystemName = tgtSystemAndEnv[0];
                                                tgtEnvironmentName = tgtSystemAndEnv[1];
                                                tempTargetTable = tgtSystemAndEnv[2];
                                                break;
                                            case 2:
                                                tgtSystemName = tgtSystemAndEnv[0];
                                                tgtEnvironmentName = tgtSystemAndEnv[1];
                                                break;
                                            case 1:
                                                tgtSystemName = tgtSystemAndEnv[0];
                                                tgtEnvironmentName = userEnvironmentName;
                                                break;
                                            default:
                                                tgtSystemName = userSystemName;
                                                tgtEnvironmentName = userEnvironmentName;
                                                break;
                                        }

                                        if (tempTargetTable.contains(Delimiter.fileTypeTableCreatedFlag)) {
                                            tempTargetTable = tempTargetTable.split(Delimiter.fileTypeTableCreatedFlag)[0];
                                            targetColumnSpecialCharatcters = true;
                                        }

                                        if (tgtTableName.contains(Delimiter.tableDelimiter)) {
                                            tgtTableName = tempTargetTable + Delimiter.tableDelimiter;
                                        } else if (tgtTableName.contains(Delimiter.storeProcdelimiter)) {
                                            tgtTableName = tempTargetTable + Delimiter.storeProcdelimiter;
                                        }
                                    }

                                    if (fileExtensionList.contains(tgtFileExtension.toLowerCase())) {
                                        isFileComponentFlag = true;
                                    }

                                    if (isFileComponentFlag) {
                                        tgtTableName = tgtTableName.replace(defSchema + ".", "");
                                    }
                                    mapSpecRow.setTargetTableName(tgtTableName);
//                                    if (targetColumnSpecialCharatcters) {
                                    if (isFileComponentFlag) {
                                        tgtColumnName = tgtColumnName.replaceAll("[^a-zA-Z0-9 \\p{L}\\.$_-]", "_");
                                    }
                                    mapSpecRow.setTargetColumnName(tgtColumnName);

                                    mapSpecRow.setTargetTableClass(targetTableClass);
                                    mapSpecRow.setTargetColumnDatatype(target.split(Delimiter.delimiter)[2]);

                                    int tgtColumnLength = 0;
                                    int tgtColumnScale = 0;
                                    int tgtColumnPrecision = 0;
                                    String tgtBusinessRule = "";
                                    try {
                                        if (targetLength >= 4 && !"NOT_DEFINED".equals(targetArray[3]) && !"".equals(targetArray[3])) {
                                            tgtColumnLength = BoxingAndUnBoxingWrapper.convertStringToInteger(targetArray[3]);
                                        }

                                        if (targetLength >= 5 && !"NOT_DEFINED".equals(targetArray[4]) && !"".equals(targetArray[4])) {
                                            tgtColumnScale = BoxingAndUnBoxingWrapper.convertStringToInteger(targetArray[4]);
                                        }

                                        if (targetLength >= 6 && !"NOT_DEFINED".equals(targetArray[5]) && !"".equals(targetArray[5])) {
                                            tgtColumnPrecision = BoxingAndUnBoxingWrapper.convertStringToInteger(targetArray[5]);
                                        }
                                        if (targetLength > 8 && !"NOT_DEFINED".equals(targetArray[8]) && !"".equals(targetArray[8])) {
                                            tgtBusinessRule = targetArray[8];
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    if (StringUtils.isBlank(srcBusinessRule) && !StringUtils.isBlank(tgtBusinessRule)) {
                                        mapSpecRow.setBusinessRule(tgtBusinessRule);
                                    }

                                    String stringTgtCoulmnLength = tgtColumnLength + "";  // done the change due to change in the method parameter datatype in EMM10.1
                                    String stringTgtColumnScale = tgtColumnScale + "";
                                    String stringTgtColumnPrecision = tgtColumnPrecision + "";

                                    mapSpecRow.setTargetColumnLength(stringTgtCoulmnLength);
                                    mapSpecRow.setTargetColumnScale(stringTgtColumnScale);
                                    mapSpecRow.setTargetColumnPrecision(stringTgtColumnPrecision);
                                    try {
                                        if (postSyncup.equals("udf")) {
                                            if (StringUtils.isNotBlank(userdefined3) && !userdefined3.equals(userSystemName) && !userdefined3.equals(userEnvironmentName) && userDF.get("tgtServer") != null && !userDF.get("tgtServer").equals("")) {
                                                mapSpecRow = UserDefinedField.setData(mapSpecRow, Integer.parseInt(userDF.get("tgtServer")), userdefined3);

                                            } else if (userDF.get("tgtServer") != null && !userDF.get("tgtServer").equals("")) {
                                                userdefined3 = "  @  ";
                                                mapSpecRow = UserDefinedField.setData(mapSpecRow, Integer.parseInt(userDF.get("tgtServer")), userdefined3);
                                            }
                                            if (StringUtils.isNotBlank(userdefined4) && !userdefined4.equals(userEnvironmentName) && !userdefined4.equals(userSystemName) && userDF.get("tgtDB") != null && !userDF.get("tgtDB").equals("")) {
                                                mapSpecRow = UserDefinedField.setData(mapSpecRow, Integer.parseInt(userDF.get("tgtDB")), userdefined4);

                                            } else if (userDF.get("tgtDB") != null && !userDF.get("tgtDB").equals("")) {
                                                userdefined4 = "  @  ";
                                                mapSpecRow = UserDefinedField.setData(mapSpecRow, Integer.parseInt(userDF.get("tgtDB")), userdefined4);
                                            }
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }

                                    if (sourceTableClass.toUpperCase().contains("LOOKUP") || "Microsoft.SCD".equalsIgnoreCase(sourceTableClass)) {
                                        srcEnvironmentName = mapName;
                                        mapSpecRow.setSourceTableName(srcTableName);

                                    }
                                    if (targetTableClass.toUpperCase().contains("LOOKUP") || "Microsoft.SCD".equalsIgnoreCase(targetTableClass)) {
                                        tgtEnvironmentName = mapName;
                                        mapSpecRow.setTargetTableName(tgtTableName);

                                    }
                                    String mergeTargetKey = mapName + "~" + tgtTableName;

                                    if (SSISController.is2014 && PrepareDataFlowLineage2014.mergeComponentMapForExtendedProperties.get(mergeTargetKey) != null) {
                                        conditionalSplitMapForExtendedProperties.put(tgtTableName, PrepareDataFlowLineage2014.mergeComponentMapForExtendedProperties.get(mergeTargetKey));
                                    }

                                    setSystemAndEnvironments(srcSystemName, srcEnvironmentName, tgtSystemName, tgtEnvironmentName, mapSpecRow, userEnvironmentName, userSystemName);
                                    String businessRule = "";
                                    if (!StringUtils.isBlank(srcBusinessRule)) {
                                        businessRule = srcBusinessRule;
                                    } else if (!StringUtils.isBlank(tgtBusinessRule)) {
                                        businessRule = tgtBusinessRule;
                                    }
                                    String setVarible = (srcEnvironmentName + srcTableName + srcColName + tgtEnvironmentName + tgtTableName + tgtColumnName + businessRule).trim().toUpperCase();

                                    if (!removeDuplicates.contains(setVarible)) {

                                        removeDuplicates.add(setVarible);
                                        if ("".equals(mapSpecRow.getSourceTableName()) || mapSpecRow.getSourceTableName() == null || removeRowcountMapSpec(mapSpecRow)) {
                                            mapSpecRow = new MappingSpecificationRow();
                                            continue;
                                        }
                                        mappingSpecifications.add(mapSpecRow);
                                    }

                                    mapSpecRow = new MappingSpecificationRow();
                                }//inner forloop
                            }

                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    List<Set<String>> extreameSrcTableAndTargetTableList = ExtreamSourceAndExtreamTarget_Util.getExtreamSourceAndTargetTablesListOfSet(mappingSpecifications);

                    Set<String> extreameSrcTableSet = null;
                    Set<String> extreamTargetSetTables = null;
                    try {
                        extreameSrcTableSet = extreameSrcTableAndTargetTableList.get(0);

                        extreamTargetSetTables = extreameSrcTableAndTargetTableList.get(1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    extendedPropertiesMap.putAll(conditionalSplitMapForExtendedProperties);
                    List<KeyValue> keyValues = MappingManagerUtilAutomation.getKeyValueMap(extendedPropertiesMap, extendedPropertiesDup);

                    appendDataFlowNameToTheIntermidiateComponetsAndOleDbSource(extreameSrcTableSet, extreamTargetSetTables, mappingSpecifications, mapName, extendedPropertiesDup, userSystemName, userDF, packageName, defSchema, hierarchyEnvName);

                    mappingSpecifications = removeDuplicateMapSpecs(mappingSpecifications);

                    mapping.setMappingSpecifications(mappingSpecifications);
                    AuditHistory auditHistory = new AuditHistory();
                    auditHistory.setCreatedBy("Administrator");
                    mapping.setAuditHistory(auditHistory);

                    mapping.setMappingName(mapName);
                    mapping.setSubjectId(subjectId);
                    mapping.setProjectId(projectId);

                    if (!dataFlowStoreProcCallSet.isEmpty()) {
                        String totlaDataFlowStoreProc = "";
                        for (String datalFlowStoreProc : dataFlowStoreProcCallSet) {
                            totlaDataFlowStoreProc = totlaDataFlowStoreProc + datalFlowStoreProc + "\n\n<br>";

                        }

                        mapping.setSourceExtractQuery(totlaDataFlowStoreProc);
                    }

                    int mappingID = MappingManagerUtilAutomation.createMappings(subjectId, mapName, acpInputParameter, mappingSpecifications, mapping, packageLog, logBuilder);

                    if (mappingID > 0) {
                        try {
                            dataFlowSet.add(extendedPropertiesMap);

                            RequestStatus req = keyValueUtil.addKeyValues(keyValues, Node.NodeType.MM_MAPPING, mappingID);
                            String reqStatus = " Extended Properties Status ===> " + req.isRequestSuccess() + "\n\n";
                            logBuilder.append(reqStatus);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    long endTime = System.currentTimeMillis();
                    long milliseconds = endTime - startTime;
                    long minutes = TimeUnit.MILLISECONDS.toMinutes(milliseconds);

                    String formatTme = String.format("%d Milliseconds = %d minutes\n", milliseconds, minutes);

                    System.out.print("Mapping Name---" + mapName + " ");
                    System.out.println("Mapping Time---" + formatTme);

                } catch (Exception e) {
                    e.printStackTrace();
                    MappingManagerUtilAutomation.writeExeceptionLog(e, "createDataFlowMappings(-,-)");

                }
            }
        }
        return logBuilder.toString();
    }

    /**
     * this method sets the system and environment name for source and target in
     * mapping specification
     *
     * @param srcSystemName
     * @param srcEnvironmentName
     * @param tgtSystemName
     * @param tgtEnvironmentName
     * @param mapSpecRow
     * @param userEnvironmentName
     * @param userSystemName
     */
    public static void setSystemAndEnvironments(String srcSystemName, String srcEnvironmentName, String tgtSystemName, String tgtEnvironmentName, MappingSpecificationRow mapSpecRow, String userEnvironmentName, String userSystemName) {
        try {
            if (!"".equals(srcSystemName)) {
                mapSpecRow.setSourceSystemName(srcSystemName);
            } else {
                mapSpecRow.setSourceSystemName(userSystemName);
            }

            if (!"".equals(srcEnvironmentName)) {
                mapSpecRow.setSourceSystemEnvironmentName(srcEnvironmentName);
            } else {
                mapSpecRow.setSourceSystemEnvironmentName(userEnvironmentName);
            }

            if (!"".equals(tgtSystemName)) {
                mapSpecRow.setTargetSystemName(tgtSystemName);
            } else {
                mapSpecRow.setTargetSystemName(userSystemName);
            }

            if (!"".equals(tgtEnvironmentName)) {
                mapSpecRow.setTargetSystemEnvironmentName(tgtEnvironmentName);
            } else {
                mapSpecRow.setSourceSystemEnvironmentName(userEnvironmentName);
            }

        } catch (Exception e) {
            MappingManagerUtilAutomation.writeExeceptionLog(e, "setSystemAndEnvironments(-,-)");

        }
    }

    /**
     * this method returns the system and environment name based on table
     * name(sync up)
     *
     * @param tableName
     * @param mapName
     * @param defSchema
     * @param acpInputParameter
     * @param databaseName
     * @param serverName
     * @param componentName
     * @param columnName
     * @return
     */
    public static String getSystemAndEnvFromDFTableName(String tableName, String mapName, String defSchema, ACPInputParameterBean acpInputParameter, String databaseName,
            String serverName, String componentName, String columnName) {

        String fileDatabaseType = "";

        String fileExtension = "";
        String columns = "";
        String excellFileName = "";
        String systemEnvironment = "";
        String dummyData = "data";
        String odataEnv = "";
        boolean isFileComponentFlag = false;

        try {
            Map<String, String> storeProcTableAndItsColumnsMap = new HashMap<>();
            if (SSISController.is2014) {
                storeProcTableAndItsColumnsMap = PrepareDataFlowLineage2014.storeProcTableAndItsColumnsMap;
            } else {
                storeProcTableAndItsColumnsMap = PrepareDataFlowLineage2008.storeProcTableAndItsColumnsMap;
            }

            tableName = MappingManagerUtilAutomation.replaceDoubleQuotesWithEmptyFromTheTableName(tableName);

            try {
                if (tableName.contains(Delimiter.fileExtensionDelimiter)) {
                    fileExtension = tableName.split(Delimiter.fileExtensionDelimiter)[1];
                    tableName = tableName.split(Delimiter.fileExtensionDelimiter)[0];
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (tableName.contains(Delimiter.fileTypeDelimiter)) {
                    fileDatabaseType = tableName.split(Delimiter.fileTypeDelimiter)[1];

                    if (fileDatabaseType.contains(Delimiter.oDataUrlDelimiter)) {
                        odataEnv = fileDatabaseType.split(Delimiter.oDataUrlDelimiter)[1];
                        fileDatabaseType = fileDatabaseType.split(Delimiter.oDataUrlDelimiter)[0];
                    }
                    tableName = tableName.split(Delimiter.fileTypeDelimiter)[0];
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if ("DSV".equalsIgnoreCase(fileDatabaseType) || "XSD".equalsIgnoreCase(fileDatabaseType) || "JSON".equalsIgnoreCase(fileDatabaseType)) {
                serverName = "  @  ";

                if (tableName.contains(Delimiter.oDataSourceDelimiter)) {
                    databaseName = tableName.split(Delimiter.oDataSourceDelimiter)[0];
                } else {
                    databaseName = tableName;
                }
                String key1 = mapName + "~" + tableName;
                if (storeProcTableAndItsColumnsMap.get(key1) != null) {

                    columns = storeProcTableAndItsColumnsMap.get(key1);
                }

                if (databaseName.contains(Delimiter.tableDelimiter)) {
                    databaseName = databaseName.split(Delimiter.tableDelimiter)[0];
                }
                isFileComponentFlag = true;

            } else if ("CSV".equalsIgnoreCase(fileDatabaseType)) {
                String serverDBTable = getDatabaseNameFromTableName(serverName, tableName, databaseName, mapName, storeProcTableAndItsColumnsMap);
                try {
                    serverName = serverDBTable.split(Delimiter.delimiter)[0];
                    databaseName = serverDBTable.split(Delimiter.delimiter)[1];
                    tableName = serverDBTable.split(Delimiter.delimiter)[2];
                    columns = serverDBTable.split(Delimiter.delimiter)[3];
                    if (serverDBTable.split(Delimiter.delimiter).length >= 5) {
                        excellFileName = serverDBTable.split(Delimiter.delimiter)[4];
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                isFileComponentFlag = true;
                tableName = tableName + Delimiter.tableDelimiter;
            }
            String schemaAndTableName = "";
            if (StringUtils.isBlank(fileDatabaseType)) {
                schemaAndTableName = getSchemaWithTableName(tableName, defSchema);
            }

            String schemaName = "";
            try {
                if (schemaAndTableName.split(Delimiter.delimiter).length >= 2) {
                    schemaName = schemaAndTableName.split(Delimiter.delimiter)[0];
                    if (tableName.contains(Delimiter.tableDelimiter) && !schemaAndTableName.split(Delimiter.delimiter)[1].contains(Delimiter.tableDelimiter)) {
                        tableName = schemaAndTableName.split(Delimiter.delimiter)[1] + Delimiter.tableDelimiter;
                    } else {
                        tableName = schemaAndTableName.split(Delimiter.delimiter)[1];
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            HashMap<String, Object> inputsMap = new HashMap<>();
            SyncUpBean syncUpBean = new SyncUpBean();
            syncUpBean.setAcpInputParameterBean(acpInputParameter);
            syncUpBean.setTableName(tableName);
            syncUpBean.setDatabaseName(databaseName);
            syncUpBean.setServerName(serverName);
            syncUpBean.setMapName(mapName);
            syncUpBean.setSchemaName(schemaName);
            syncUpBean.setColumns(columns);
            syncUpBean.setIsFileTypeComponent(isFileComponentFlag);
            syncUpBean.setFileDatabaseType(fileDatabaseType);
            syncUpBean.setExcellFileName(excellFileName);
            if (StringUtils.isNotBlank(odataEnv)) {
                inputsMap.put("oDataEnvironmentName", odataEnv);
            }
            if (isFileComponentFlag) {
                inputsMap.put("componentName", componentName);
                inputsMap.put("fileColumnName", columnName);
            }

            inputsMap = SyncUpUtil.prapareMetaDataSyncUpInputsMap(syncUpBean, inputsMap);

//            if (acpInputParameter.isIsVolumeSyncUp()) {
//                systemEnvironment = com.erwin.cfx.connectors.json.syncup.v1.SyncupWithServerDBSchamaSysEnvCPT.newmetasync(inputsMap);
//            } else {
            systemEnvironment = SyncupWithServerDBSchamaSysEnvCPT.newmetasync(inputsMap);
//            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getSystemAndEnvFromDFTableName(-,-)");
        }

        return systemEnvironment + Delimiter.systemAndDBSeparator + serverName
                + Delimiter.systemAndDBSeparator + databaseName + Delimiter.systemAndDBSeparator + fileExtension
                + Delimiter.systemAndDBSeparator + dummyData;
    }

    /**
     * this method returns database name (File name) based on table name for
     * excel component
     *
     * @param serverName
     * @param tableName
     * @param databaseName
     * @param mapName
     * @param storeProcTableAndItsColumnsMap
     * @return
     */
    public static String getDatabaseNameFromTableName(String serverName, String tableName, String databaseName, String mapName, Map<String, String> storeProcTableAndItsColumnsMap) {
        serverName = "  @  ";
        String excellFileName = "";
        String dummyData = "testData";

        String key = mapName + "~" + tableName;
        String coulmns = "";
        if (storeProcTableAndItsColumnsMap.get(key) != null) {
            coulmns = storeProcTableAndItsColumnsMap.get(key);
        }

        tableName = tableName.replace("'", "").replace("$", "");

        if (tableName.contains(Delimiter.tableDelimiter)) {
            tableName = tableName.split(Delimiter.tableDelimiter)[0];
        }

        try {
            tableName = tableName.replaceAll("[^a-zA-Z0-9 \\p{L}_\\-$.]", "_");

            if (tableName.contains(".")) {
                excellFileName = tableName.split("\\.")[0];
                tableName = tableName.split("\\.")[1];

            }
            databaseName = tableName;
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getDatabaseNameFromTableName(-,-)");
        }
        return serverName + Delimiter.delimiter + databaseName + Delimiter.delimiter + tableName + Delimiter.delimiter + coulmns + Delimiter.delimiter + excellFileName + Delimiter.delimiter + dummyData;
    }

    /**
     * this method gets the store procedure details into the mapping source
     * extract SQL
     *
     * @param extendedPropertiesMap
     * @param dataFlowStoreProcCallSet
     * @return
     */
    public static HashMap<String, String> getTheExecuteCallSFromTheExtendedPropertiesMap(HashMap<String, String> extendedPropertiesMap, Set<String> dataFlowStoreProcCallSet) {
        HashMap<String, String> modifiedExtendedPropertiesMap = new HashMap<>();

        for (Map.Entry<String, String> mapEntrySet : extendedPropertiesMap.entrySet()) {

            String key = mapEntrySet.getKey();
            String value = mapEntrySet.getValue();
            key = key.replace("\\(", "").replace("\\)", "");

            String componentName = "";
            try {
                if (key.contains(Delimiter.ed_ge_Delimiter) && key.split(Delimiter.ed_ge_Delimiter).length >= 2) {
                    componentName = key.split(Delimiter.ed_ge_Delimiter)[1].replace(Delimiter.storeProcdelimiter.toUpperCase(), "").replace(Delimiter.storeProcdelimiter, "");
                }
                if (componentName.contains("$")) {
                    componentName = componentName.substring(0, componentName.lastIndexOf("$"));

                }
            } catch (Exception e) {
                e.printStackTrace();
                MappingManagerUtilAutomation.writeExeceptionLog(e, "getTheExecuteCallSFromTheExtendedPropertiesMap(-,-)");

            }

            StringBuilder componentNameAndExecuteCall = new StringBuilder();
            componentNameAndExecuteCall.append(componentName + " :- " + "\n<br>");
            componentNameAndExecuteCall.append("*********************" + "\n<br>");
            componentNameAndExecuteCall.append(value.split(Delimiter.delimiter)[0] + "\n<br>");
            try {
                if (value.toUpperCase().contains("EXEC") || value.toUpperCase().contains("EXECUTE")) {
                    dataFlowStoreProcCallSet.add(componentNameAndExecuteCall.toString());
                }

                modifiedExtendedPropertiesMap.put(key, value);

            } catch (Exception e) {
                e.printStackTrace();
                MappingManagerUtilAutomation.writeExeceptionLog(e, "getTheExecuteCallSFromTheExtendedPropertiesMap(-,-)");

            }

        }

        return modifiedExtendedPropertiesMap;
    }

    /**
     * this method returns table name along with the schema
     *
     * @param tableName
     * @param defSchema
     * @return
     */
    public static String getSchemaWithTableName(String tableName, String defSchema) {
        ArrayList<String> targetDetailedTableNameList = SyncupWithServerDBSchamaSysEnvCPT.getTableName(tableName, defSchema);
        String schemaName = "";

        for (String targetDetailedTableName : targetDetailedTableNameList) {
            try {
                String[] targetDetailedTableNameArray = targetDetailedTableName.split(Delimiter.delimiter);
                int length = targetDetailedTableNameArray.length;

                if (length >= 4) {
                    schemaName = targetDetailedTableName.split(Delimiter.delimiter)[2];
                    tableName = targetDetailedTableName.split(Delimiter.delimiter)[3];
                }

                if (StringUtils.isBlank(schemaName) && tableName.contains(Delimiter.storeProcdelimiter)) {
                    tableName = defSchema + "." + tableName;
                    schemaName = defSchema;
                }
            } catch (Exception e) {
                e.printStackTrace();
                MappingManagerUtilAutomation.writeExeceptionLog(e, "getSchemaWithTableName(-,-)");
            }
        }

        return schemaName + Delimiter.delimiter + tableName;
    }

    /**
     * this method removes particular mapping specification if it contains row
     * count input in either source or target components
     *
     * @param mappingSpecRow
     * @return
     */
    public static boolean removeRowcountMapSpec(MappingSpecificationRow mappingSpecRow) {
        if ("Row Count Input 1".equalsIgnoreCase(mappingSpecRow.getSourceColumnName()) || "Row Count Output 1".equalsIgnoreCase(mappingSpecRow.getSourceColumnName())) {
            if ("Row Count Input 1".equalsIgnoreCase(mappingSpecRow.getSourceColumnName()) && "Row Count Input 1".equalsIgnoreCase(mappingSpecRow.getTargetColumnName())) {
                return false;
            }
            if ("Row Count Output 1".equalsIgnoreCase(mappingSpecRow.getSourceColumnName()) && "Row Count Output 1".equalsIgnoreCase(mappingSpecRow.getTargetColumnName())) {
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * this method will add map name(data flow name) and package name to the
     * data flow intermediate components
     *
     * @param extreamSourceTableSet
     * @param extreamTargetTableSet
     * @param mappingSpecifications
     * @param mapName
     * @param extendedpropMapDup
     * @param defSystem
     * @param userDF
     * @param packageName
     * @param defaultSchema
     * @param hierarchyEnvironementName
     */
    public static void appendDataFlowNameToTheIntermidiateComponetsAndOleDbSource(Set<String> extreamSourceTableSet, Set<String> extreamTargetTableSet, ArrayList<MappingSpecificationRow> mappingSpecifications, String mapName, Map<String, String> extendedpropMapDup, String defSystem, Map<String, String> userDF, String packageName, String defaultSchema, String hierarchyEnvironementName) {

        try {

            final String hierarchyEnvironementNameForMiddleComponents = returnHirerchalEnvName(hierarchyEnvironementName, packageName);

            mappingSpecifications.stream().forEach(mappingSpecificationRow -> {
                String businessRule = mappingSpecificationRow.getBusinessRule();
                String srcTableName = mappingSpecificationRow.getSourceTableName();
                String srcColName = mappingSpecificationRow.getSourceColumnName();
                String srcSystemName = mappingSpecificationRow.getSourceSystemName();
                String srcEnvironmentName = mappingSpecificationRow.getSourceSystemEnvironmentName();
                String tgtTableName = mappingSpecificationRow.getTargetTableName();
                String tgtColumnName = mappingSpecificationRow.getTargetColumnName();
                String tgtSystem = mappingSpecificationRow.getTargetSystemName();
                String tgtEnvironmentName = mappingSpecificationRow.getTargetSystemEnvironmentName();
                boolean componentflag = false;

                String mapWitPackageName = mapName + "__PKG__" + packageName + "__";

                if (!extreamSourceTableSet.contains(srcTableName.toUpperCase()) && !extreamTargetTableSet.contains(srcTableName.toUpperCase()) && !srcTableName.contains(Delimiter.tableDelimiter) && !srcTableName.contains(Delimiter.queryTableDelimiter)) {

                    srcTableName = returnTableNameWithOutDelimters(srcTableName, componentflag, defaultSchema);
                    srcTableName = mapWitPackageName + srcTableName;

                    String userdefined = "#";
                    try {
                        if (userDF.get("srcServer") != null && !userDF.get("srcServer").equals("")) {
                            mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("srcServer").toString()), userdefined);
                        }
                        if (userDF.get("srcDB") != null && !userDF.get("srcDB").equals("")) {
                            mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("srcDB").toString()), userdefined);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    mappingSpecificationRow.setSourceSystemEnvironmentName(hierarchyEnvironementNameForMiddleComponents);

                } else if (extreamSourceTableSet.contains(srcTableName.toUpperCase()) && extendedpropMapDup.containsKey(srcTableName) && !srcTableName.contains(Delimiter.storeProcdelimiter)) {
                    componentflag = true;
                    srcTableName = returnTableNameWithOutDelimters(srcTableName, componentflag, defaultSchema);

                    srcTableName = mapWitPackageName + srcTableName;
                    mappingSpecificationRow.setSourceTableName(srcTableName);
                    srcSystemName = defSystem;
                    srcEnvironmentName = hierarchyEnvironementNameForMiddleComponents;
                    mappingSpecificationRow.setSourceSystemName(defSystem);
                    mappingSpecificationRow.setSourceSystemEnvironmentName(hierarchyEnvironementNameForMiddleComponents);
                }
                if (!extreamSourceTableSet.contains(tgtTableName.toUpperCase()) && !extreamTargetTableSet.contains(tgtTableName.toUpperCase()) && !tgtTableName.contains(Delimiter.tableDelimiter) && !tgtTableName.contains(Delimiter.storeProcdelimiter)) {
                    tgtTableName = returnTableNameWithOutDelimters(tgtTableName, componentflag, defaultSchema);
                    tgtTableName = mapWitPackageName + tgtTableName;
                    if (!tgtTableName.contains(Delimiter.queryTableDelimiter)) {
                        String userdefined = "#";
                        try {
                            if (userDF.get("tgtServer") != null && !userDF.get("tgtServer").equals("")) {
                                mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("tgtServer").toString()), userdefined);
                            }
                            if (userDF.get("tgtDB") != null && !userDF.get("tgtDB").equals("")) {

                                mappingSpecificationRow = UserDefinedField.setData(mappingSpecificationRow, Integer.parseInt(userDF.get("tgtDB").toString()), userdefined);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    mappingSpecificationRow.setTargetSystemEnvironmentName(hierarchyEnvironementNameForMiddleComponents);
                }
                srcTableName = returnTableNameWithOutDelimters(srcTableName, componentflag, defaultSchema);
                tgtTableName = returnTableNameWithOutDelimters(tgtTableName, componentflag, defaultSchema);
                String dataflowSpecMapKey = "";
                dataflowSpecMapKey = srcSystemName + srcEnvironmentName + srcTableName + srcColName + tgtSystem + tgtEnvironmentName + tgtTableName + tgtColumnName;
                if (!StringUtils.isBlank(businessRule)) {
                    dataFlowSpecificationMap.put(dataflowSpecMapKey, businessRule);
                }
                mappingSpecificationRow.setSourceTableName(srcTableName);
                mappingSpecificationRow.setTargetTableName(tgtTableName);
            });

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "appendDataFlowNameToTheIntermidiateComponetsAndOleDbSource(-,-)");

        }

    }

    /**
     * this method removes all the Delimiters present within the table component
     *
     * @param tableName
     * @param componentflag
     * @param defaultSchema
     * @return
     */
    public static String returnTableNameWithOutDelimters(String tableName, boolean componentflag, String defaultSchema) {
        try {
            if (tableName.contains(Delimiter.tableDelimiter)) {
                int length = tableName.split(Delimiter.tableDelimiter).length;
                if (length >= 1) {
                    tableName = tableName.split(Delimiter.tableDelimiter)[0];
                }

            }
            if (tableName.contains(Delimiter.storeProcdelimiter)) {
                int length = tableName.split(Delimiter.storeProcdelimiter).length;
                if (length >= 1) {
                    tableName = tableName.split(Delimiter.storeProcdelimiter)[0];
                }

            }
            if (componentflag) {
                tableName = tableName.replaceAll("[^a-zA-Z0-9 \\p{L}\\._-]", "_");
            }
            if (tableName.contains(Delimiter.oDataSourceDelimiter)) {
                tableName = tableName.replace(Delimiter.oDataSourceDelimiter, "").replace(defaultSchema + ".", "");
            }
            if (tableName.contains(Delimiter.queryTableDelimiter)) {
                tableName = tableName.replace(Delimiter.queryTableDelimiter, "");
            }
            if (tableName.contains(Delimiter.ed_ge_Delimiter)) {
                int length = tableName.split(Delimiter.ed_ge_Delimiter).length;
                if (length >= 1) {
                    tableName = tableName.split(Delimiter.ed_ge_Delimiter)[0];
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "returnTableNameWithOutDelimters(-,-)");
        }
        return tableName;
    }

    /**
     * this method removes duplicate specifications from the mapping
     * specifications
     *
     * @param mappingSpecifications
     * @return ArrayList<MappingSpecificationRow>
     */
    public static ArrayList<MappingSpecificationRow> removeDuplicateMapSpecs(ArrayList<MappingSpecificationRow> mappingSpecifications) {
        ArrayList<MappingSpecificationRow> newMapSpecs = new ArrayList<>();

        try {

            for (MappingSpecificationRow mappingSpecificationRow : mappingSpecifications) {

                String businessRule = mappingSpecificationRow.getBusinessRule();
                String srcTableName = mappingSpecificationRow.getSourceTableName();
                String srcColName = mappingSpecificationRow.getSourceColumnName();
                String srcSystemName = mappingSpecificationRow.getSourceSystemName();
                String srcEnvironmentName = mappingSpecificationRow.getSourceSystemEnvironmentName();
                String tgtTableName = mappingSpecificationRow.getTargetTableName();
                String tgtColumnName = mappingSpecificationRow.getTargetColumnName();
                String tgtSystem = mappingSpecificationRow.getTargetSystemName();
                String tgtEnvironmentName = mappingSpecificationRow.getTargetSystemEnvironmentName();

                String dataflowSpecMapKey = "";
                dataflowSpecMapKey = srcSystemName + srcEnvironmentName + srcTableName + srcColName + tgtSystem + tgtEnvironmentName + tgtTableName + tgtColumnName;

                if (StringUtils.isBlank(businessRule) && dataFlowSpecificationMap.containsKey(dataflowSpecMapKey)) {

                } else {
                    newMapSpecs.add(mappingSpecificationRow);
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "removeDuplicateMapSpecs(-,-)");
        }
        return newMapSpecs;
    }

    /**
     * this method return column name for the data conversion component
     *
     * @param dataConversionColumnMap
     * @param mapName
     * @param srcColName
     * @param srcTableName
     * @return String
     */
    public static String returnSourceColumnName(Map<String, String> dataConversionColumnMap, String mapName, String srcColName, String srcTableName) {
        String newSourceColumn = "";
        try {
            if (dataConversionColumnMap != null && dataConversionColumnMap.get(mapName + Delimiter.delimiter + srcColName) != null) {
                newSourceColumn = dataConversionColumnMap.get(mapName + Delimiter.delimiter + srcColName);
                if (dataConversionColumnMap.get(mapName + Delimiter.delimiter + srcTableName + Delimiter.delimiter + newSourceColumn) != null) {
                    srcColName = dataConversionColumnMap.get(mapName + Delimiter.delimiter + srcTableName + Delimiter.delimiter + newSourceColumn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "returnSourceColumnName(-,-)");
        }
        return srcColName;
    }

    /**
     *
     * @param hirerchyName
     * @param packageName
     *
     * @return
     */
    public static String returnHirerchalEnvName(String hirerchyName, String packageName) {
        if (StringUtils.isNotBlank(hirerchyName)) {
            hirerchyName = hirerchyName + "__" + packageName;
        } else {
            hirerchyName = packageName;
        }

        return hirerchyName;
    }

}
