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
import com.erwin.cfx.connectors.ssis.generic.util.BoxingAndUnBoxingWrapper;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.ExtreamSourceAndExtreamTarget_Util;
import com.erwin.cfx.connectors.ssis.generic.util.MappingManagerUtilAutomation;
import com.erwin.cfx.connectors.ssis.generic.util.SqlParserUtil;
import com.erwin.cfx.connectors.ssis.generic.util.SyncUpUtil;
import com.icc.util.RequestStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 27-08-2021
 */
public class CreateControlFlowMaps {

    CreateControlFlowMaps() {
    }

    private static final String ERWINDISABLED = "_ERWINDISABLED";
    public static Set<String> controlFlowStoreProcCallsSet = new HashSet<>();

    /**
     * this method creates mappings for control flow components
     *
     * @param controlFlowLevelLineageMap
     * @param subjectId
     * @param projectId
     * @param acpInputParameterBean
     * @param controlFlowSet
     * @param packageLog
     */
    public static String createControlFlowLevelMaps(Map<String, List<HashMap<String, String>>> controlFlowLevelLineageMap, int subjectId, int projectId, ACPInputParameterBean acpInputParameterBean, Set<Map<String, String>> controlFlowSet, String packageLog, String hirarchyFolderName) {

        StringBuilder logBuilder = new StringBuilder();
        if (controlFlowLevelLineageMap != null) {
            controlFlowLevelLineageMap.forEach((key, value) -> {
                try {
                    long startTime = System.currentTimeMillis();
                    Map<String, String> sourceAndTargetMap = new LinkedHashMap<>();
                    ArrayList<MappingSpecificationRow> mappingSpecifications = new ArrayList<>();
                    Mapping mapping = new Mapping();
                    String mapName = key;
                    if (StringUtils.isNotBlank(mapName) && mapName.length() >= 300) {
                        mapName = mapName.substring(0, 295) + "...";
                    }
                    List<HashMap<String, String>> listOfHashMap = (ArrayList<HashMap<String, String>>) value;
                    Map<String, String> existingLineageMap = listOfHashMap.get(0);
                    controlFlowStoreProcCallsSet = new HashSet<>();

                    KeyValueUtil keyValueUtil = acpInputParameterBean.getKeyValueUtil();

                    String systemName = acpInputParameterBean.getDefaultSystemName();
                    String environmentName = acpInputParameterBean.getDefaultEnvrionmentName();
                    String packageName = acpInputParameterBean.getInputFileName();
                    existingLineageMap.forEach((target, source) -> {

                        String[] checkMultiSource = source.split("_ADS_");

                        for (String sourceComponent : checkMultiSource) {
                            if (sourceAndTargetMap.get(sourceComponent) != null) {
                                String existingTarget = sourceAndTargetMap.get(sourceComponent);
                                sourceAndTargetMap.put(sourceComponent, existingTarget + Delimiter.delimiter + target);
                            } else {
                                sourceAndTargetMap.put(sourceComponent, target);
                            }
                        }

                    });
                    Map<String, String> lineageMap = getEnabledLineageHashMap(sourceAndTargetMap);
                    String hierarchyEnvName = CreateDataFlowMappings.returnHirerchalEnvName(hirarchyFolderName, packageName);

                    Map<String, String> extendedpropMap = new LinkedHashMap<>(listOfHashMap.get(1));
                    extendedpropMap = addStoreprocNameAsMappingSpecificationInControlFlowMappings(extendedpropMap, mappingSpecifications, mapName, controlFlowStoreProcCallsSet, acpInputParameterBean, hierarchyEnvName);

                    mappingSpecifications = prepareControlFlowSpecLists(lineageMap, mapName, systemName, environmentName, mappingSpecifications, hierarchyEnvName);

                    int mappingID = 0;
                    RequestStatus status = null;
                    mapName = mapName.replaceAll("[^a-zA-Z0-9 \\p{L}_-]", "_");

//                    updateIntemediateEnvironmentName(hierarchyEnvName, mappingSpecifications);
                    mapping.setMappingSpecifications(mappingSpecifications);
                    AuditHistory auditHistory = new AuditHistory();
                    auditHistory.setCreatedBy("Administrator");
                    mapping.setAuditHistory(auditHistory);

                    mapping.setMappingName(mapName);

                    mapping.setSubjectId(subjectId);
                    mapping.setProjectId(projectId);

                    if (!controlFlowStoreProcCallsSet.isEmpty()) {

                        String totalControlFlowStoreProc = "";
                        for (String controlFlowStoreProc : controlFlowStoreProcCallsSet) {
                            totalControlFlowStoreProc = totalControlFlowStoreProc + controlFlowStoreProc + "\n\n<br>";

                        }
                        mapping.setSourceExtractQuery(totalControlFlowStoreProc);
                    }

                    mappingID = MappingManagerUtilAutomation.createMappings(subjectId, mapName, acpInputParameterBean, mappingSpecifications, mapping, packageLog, logBuilder);

                    if (mappingID > 0) {
                        try {

                            if (!extendedpropMap.isEmpty()) {
                                List<KeyValue> keyValues = MappingManagerUtilAutomation.getKeyValueMap(extendedpropMap, new HashMap<>());
                                controlFlowSet.add(SqlParserUtil.getQuerySet(listOfHashMap.get(1)));

                                RequestStatus req = keyValueUtil.addKeyValues(keyValues, Node.NodeType.MM_MAPPING, mappingID);

                                String reqStatus = " Extended Properties Status ===> " + req.isRequestSuccess() + "\n\n";
                                logBuilder.append(reqStatus);
                            }

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
                    MappingManagerUtilAutomation.writeExeceptionLog(e, "createControlFlowLevelMaps(-,-)");

                }
            }
            );
        }

        return logBuilder.toString();
    }

    /**
     * this method prepares the list of mapping specifications for particular
     * mappings
     *
     * @param lineageMap
     * @param mapName
     * @param systemName
     * @param environmentName
     * @param mappingSpecifications
     * @param hierarchyEnvName
     * @return
     */
    public static ArrayList<MappingSpecificationRow> prepareControlFlowSpecLists(Map<String, String> lineageMap, String mapName, String systemName, String environmentName, ArrayList<MappingSpecificationRow> mappingSpecifications, String hierarchyEnvName) {

        lineageMap.entrySet().forEach(entrySet -> {
            String source = entrySet.getKey();
            String target = entrySet.getValue();

            String[] checkMultiTarget = target.split(Delimiter.delimiter);

            for (String target1 : checkMultiTarget) {
                try {
                    if (source.contains(mapName + "\\")) {
                        source = source.substring(source.lastIndexOf("\\") + 1);

                    } else {
                        source = source.replace("Package\\", "");
                    }

                    if (target1.contains(mapName + "\\")) {
                        target1 = target1.substring(target1.lastIndexOf("\\") + 1);
                    } else {
                        target1 = target1.replace("Package\\", "");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    MappingManagerUtilAutomation.writeExeceptionLog(e, "prepareControlFlowSpecLists(-,-)");

                }

                MappingSpecificationRow mapSpecRow = new MappingSpecificationRow();

                mapSpecRow.setTargetSystemName(systemName);
                mapSpecRow.setTargetSystemEnvironmentName(hierarchyEnvName);

                mapSpecRow.setSourceSystemName(systemName);
                mapSpecRow.setSourceSystemEnvironmentName(hierarchyEnvName);

                mapSpecRow.setTargetTableName(target1);
                mapSpecRow.setTargetColumnName("#Dummy#");
                mapSpecRow.setTargetTableClass("Tgt");
                mapSpecRow.setTargetColumnDatatype("int");
                mapSpecRow.setTargetColumnLength(0 + "");
                mapSpecRow.setTargetColumnScale(0 + "");
                mapSpecRow.setTargetColumnPrecision(0 + "");

                mapSpecRow.setSourceTableName(source);
                mapSpecRow.setSourceColumnName("#Dummy#");
                mapSpecRow.setSourceTableClass("src");
                mapSpecRow.setSourceColumnDatatype("int");
                mapSpecRow.setSourceColumnLength(0 + "");
                mapSpecRow.setSourceColumnScale(0 + "");
                mapSpecRow.setSourceColumnPrecision(0 + "");

                mappingSpecifications.add(mapSpecRow);
            }
        });
        return mappingSpecifications;
    }

    /**
     * this method prepares the lineage only for enabled control flow components
     *
     * @param sourceAndTargetMap
     * @return
     */
    public static Map<String, String> getEnabledLineageHashMap(Map<String, String> sourceAndTargetMap) {
        Map<String, String> lineageMap = new LinkedHashMap<>();
        Set<String> keySet = sourceAndTargetMap.keySet();
        Set<String> valuesSet = new HashSet<>(sourceAndTargetMap.values());
        Iterator<String> extreamSourceSetItr = keySet.iterator();
        Set<String> extreamSourceSet = new LinkedHashSet<>();

        while (extreamSourceSetItr.hasNext()) {
            String key = extreamSourceSetItr.next();
            boolean flag = getExtreamSource(key, valuesSet);
            if (flag && !key.contains(ERWINDISABLED)) {
                extreamSourceSet.add(key);
            }
        }
        for (Map.Entry<String, String> entry : sourceAndTargetMap.entrySet()) {
            String source = entry.getKey();
            String target = entry.getValue();
            if (target.contains(Delimiter.delimiter)) {
                String[] spiltTarget = target.split(Delimiter.delimiter);
                for (String spiltTgt : spiltTarget) {
                    if (spiltTgt.contains(ERWINDISABLED)) {
                        if (extreamSourceSet.contains(source)) {
                            lineageMap.put(source, " ");
                        }
                        continue;
                    } else if (source.contains(ERWINDISABLED)) {
                        String prevoiusEnableSource = getPrevoiusEnabledSource(source, sourceAndTargetMap);
                        if (!prevoiusEnableSource.equals("") && !prevoiusEnableSource.contains(ERWINDISABLED)) {
                            if (lineageMap.get(prevoiusEnableSource) != null) {
                                String oldValue = lineageMap.get(prevoiusEnableSource);
                                lineageMap.put(prevoiusEnableSource, oldValue + Delimiter.delimiter + spiltTgt);
                            } else {
                                lineageMap.put(prevoiusEnableSource, spiltTgt);
                            }
                        } else if (prevoiusEnableSource.equals("") && !spiltTgt.equals(" ")) {
                            if (lineageMap.get(" ") != null) {
                                String oldValue = lineageMap.get(" ");
                                lineageMap.put(" ", oldValue + Delimiter.delimiter + spiltTgt);
                            } else {
                                lineageMap.put(" ", spiltTgt);
                            }
                        }
                    } else {
                        if (lineageMap.get(source) != null) {
                            String oldValue = lineageMap.get(source);
                            lineageMap.put(source, oldValue + Delimiter.delimiter + spiltTgt);
                        } else {
                            lineageMap.put(source, spiltTgt);
                        }
                    }
                }
            } else {
                if (target.contains(ERWINDISABLED)) {
                    try {
                        if (extreamSourceSet.contains(source)) {
                            String targetDisabled = sourceAndTargetMap.get(source);
                            boolean disbledFlag = true;
                            while (disbledFlag) {
                                if (targetDisabled == null) {
                                    lineageMap.put(source, " ");
                                    disbledFlag = false;
                                } else if (targetDisabled.contains(ERWINDISABLED)) {
                                    String[] spiltTargetDisbled = targetDisabled.split(Delimiter.delimiter);
                                    for (String spiltMultiTarget : spiltTargetDisbled) {
                                        targetDisabled = spiltMultiTarget;
                                        targetDisabled = sourceAndTargetMap.get(targetDisabled);
                                    }
                                } else {
                                    disbledFlag = false;
                                }
                            }
                            if (!StringUtils.isBlank(targetDisabled)) {
                                lineageMap.put(source, targetDisabled);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    continue;
                } else if (source.contains(ERWINDISABLED)) {
                    source = getPrevoiusEnabledSource(source, sourceAndTargetMap);
                    if (source.contains(ERWINDISABLED)) {
                        source = "";
                    }
                    if (!source.equals("") && !source.contains(ERWINDISABLED)) {
                        lineageMap.put(source, target);
                    } else if (source.equals("") && !target.equals("")) {
                        lineageMap.put("", target);
                    }
                } else {
                    if (lineageMap.get(source) != null) {
                        String oldValue = lineageMap.get(source);
                        lineageMap.put(source, oldValue + Delimiter.delimiter + target);
                    } else {
                        lineageMap.put(source, target);
                    }
                }
            }
        }
        return lineageMap;
    }

    /**
     * this method will return the previous enabled source back to the method
     * calling
     *
     * @author : Dinesh Arasankala
     * @param source
     * @param sourceAndTargetMap
     * @return enable previous source
     */
    public static String getPrevoiusEnabledSource(String source, Map<String, String> sourceAndTargetMap) {

        for (Map.Entry<String, String> sourceAndTargetEntry : sourceAndTargetMap.entrySet()) {
            String prevoiusSource = sourceAndTargetEntry.getKey();
            String prevoiusTarget = sourceAndTargetEntry.getValue();
            if (prevoiusTarget.contains(Delimiter.delimiter)) {
                String[] spiltTarget = prevoiusTarget.split(Delimiter.delimiter);
                for (String spilttargetData : spiltTarget) {
                    if (source.trim().equalsIgnoreCase(spilttargetData)) {

                        if (prevoiusSource.contains(ERWINDISABLED)) {
                            getPrevoiusEnabledSource(prevoiusSource, sourceAndTargetMap);
                        } else {
                            return prevoiusSource;
                        }

                    }

                }

            } else {
                if (source.trim().equalsIgnoreCase(prevoiusTarget)) {

                    if (prevoiusSource.contains(ERWINDISABLED)) {
                        source = getPrevoiusEnabledSource(prevoiusSource, sourceAndTargetMap);
                        return source;
                    } else {
                        return prevoiusSource;
                    }

                }

            }
        }

        return source;
    }

    /**
     * this method will tell whether the given component is extream source or
     * not
     *
     * @param key
     * @param valueSet
     * @return true or false
     */
    public static boolean getExtreamSource(String key, Set<String> valueSet) {

        Iterator<String> valueSetItr = valueSet.iterator();

        while (valueSetItr.hasNext()) {
            String value = valueSetItr.next();

            if (value.contains(Delimiter.delimiter)) {
                String spiltValue[] = value.split(Delimiter.delimiter);
                for (String spiltValueData : spiltValue) {
                    if (key.equalsIgnoreCase(spiltValueData)) {
                        return false;
                    }
                }

            } else {
                if (key.equalsIgnoreCase(value)) {
                    return false;
                }
            }

        }
        return true;
    }

    /**
     * this method adds the store procedure as a mapping specification in the
     * control flow mapping
     *
     * @param extendedPropMap
     * @param mappingSpecificationRows
     * @param mapName
     * @param controlFlowStoreProcCallsSet
     * @param acpInputParameterBean
     * @return
     */
    public static Map<String, String> addStoreprocNameAsMappingSpecificationInControlFlowMappings(Map<String, String> extendedPropMap, ArrayList<MappingSpecificationRow> mappingSpecificationRows, String mapName, Set<String> controlFlowStoreProcCallsSet, ACPInputParameterBean acpInputParameterBean, String heirarchyEnvName) {
        Map<String, String> modifiedExtendedPropMap = new HashMap<>();
        boolean flag = true;
        Map<String, String> userDF = new HashMap<>();

        String defaultSchemaName = acpInputParameterBean.getDefaultSchema();
        try {
            for (Map.Entry<String, String> extendedPropMapEntry : extendedPropMap.entrySet()) {
                String componentName = extendedPropMapEntry.getKey();
                String query = extendedPropMapEntry.getValue();

                if (!StringUtils.isBlank(query) && (query.toUpperCase().contains("EXEC") || query.toUpperCase().contains("EXECUTE"))) {
                    flag = true;
                    String executeCall = query.split(Delimiter.delimiter)[0];
                    String databaseName = "";
                    String serverName = "";
                    String storeProcedureName = "";

                    String[] queryArray = query.split(Delimiter.ed_ge_Delimiter);
                    int edgeLength = queryArray.length;
                    if (edgeLength == 2) {
                        databaseName = queryArray[0];
                        serverName = queryArray[1];
                        databaseName = databaseName.replace(executeCall + Delimiter.delimiter, "");
                    } else if (edgeLength == 1) {
                        databaseName = queryArray[0];
                        databaseName = databaseName.replace(executeCall + Delimiter.delimiter, "");
                    }

                    String queryAndStoreProcName = SqlParserUtil.makeStorePrcedureNameAsComponentName(executeCall, storeProcedureName);

                    if (queryAndStoreProcName.contains(Delimiter.delimiter)) {
                        int length = queryAndStoreProcName.split(Delimiter.delimiter).length;
                        if (length == 2) {
                            query = queryAndStoreProcName.split(Delimiter.delimiter)[0];
                            storeProcedureName = queryAndStoreProcName.split(Delimiter.delimiter)[1];
                        } else if (length == 1) {
                            query = queryAndStoreProcName.split(Delimiter.delimiter)[0];
                        }
                    }
                    String schemaAndStroreProcName = SyncUpUtil.returnStroreProcedureName(modifiedExtendedPropMap, storeProcedureName, componentName, query, defaultSchemaName);
                    String schemaName = "";
                    try {

                        String[] schemaAndStroreProcNameArray = schemaAndStroreProcName.split(Delimiter.delimiter);
                        int length = schemaAndStroreProcNameArray.length;
                        if (length == 3) {
                            storeProcedureName = schemaAndStroreProcNameArray[0];
                            schemaName = schemaAndStroreProcNameArray[1];
                            flag = BoxingAndUnBoxingWrapper.convertStringToBoolean(schemaAndStroreProcNameArray[2]);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    StringBuilder componentNameAndExecuteCall = new StringBuilder();

                    if (componentName.contains("$")) {
                        componentName = componentName.substring(0, componentName.lastIndexOf("$"));

                    }
                    if (flag) {
                        componentNameAndExecuteCall.append(componentName + " :- " + "\n<br>");
                        componentNameAndExecuteCall.append("*********************" + "\n<br>");
                        componentNameAndExecuteCall.append(executeCall + "\n<br>");

                        controlFlowStoreProcCallsSet.add(componentNameAndExecuteCall.toString());
                        HashMap<String, Object> inputsMap = new HashMap<>();
                        SyncUpBean syncUpBean = new SyncUpBean();
                        syncUpBean.setAcpInputParameterBean(acpInputParameterBean);
                        syncUpBean.setTableName(storeProcedureName);
                        syncUpBean.setDatabaseName(databaseName);
                        syncUpBean.setServerName(serverName);
                        syncUpBean.setMapName(mapName);
                        syncUpBean.setSchemaName(schemaName);
                        syncUpBean.setColumns("");
                        syncUpBean.setIsFileTypeComponent(false);
                        syncUpBean.setFileDatabaseType("");
                        syncUpBean.setExcellFileName("");
                        syncUpBean.setHeirarchyFolderEnvName(heirarchyEnvName);

                        inputsMap = SyncUpUtil.prapareMetaDataSyncUpInputsMap(syncUpBean, inputsMap);

                        String systemEnvironment = "";
//                        if (acpInputParameterBean.isIsVolumeSyncUp()) {
//                            systemEnvironment = com.erwin.cfx.connectors.json.syncup.v1.SyncupWithServerDBSchamaSysEnvCPT.newmetasync(inputsMap);
//                        } else {
                        systemEnvironment = SyncupWithServerDBSchamaSysEnvCPT.newmetasync(inputsMap);
//                        }

                        if (storeProcedureName.contains(Delimiter.storeProcdelimiter)) {
                            storeProcedureName = storeProcedureName.split(Delimiter.storeProcdelimiter)[0];
                        }
                        syncUpBean.setTableName(storeProcedureName);
                        MappingSpecificationRow mappingSpecificationRow = SyncUpUtil.returnMapSpecRowForStoreProcedure(systemEnvironment, syncUpBean, componentName);
                        mappingSpecificationRows.add(mappingSpecificationRow);
                    }
                }
                if (!StringUtils.isBlank(query)) {
                    modifiedExtendedPropMap.put(componentName, query);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "addStoreprocNameAsMappingSpecificationInControlFlowMappings(-,-)");

        }

        return modifiedExtendedPropMap;
    }

    /**
     * in this method we are overirding default env Name to folder name for
     * intermediate control flow components
     *
     * @param hirarachyFolderName
     * @param mappingSpecifications
     */
    public static void updateIntemediateEnvironmentName(String hirarachyFolderName, ArrayList<MappingSpecificationRow> mappingSpecifications) {
        List<Set<String>> extreameSrcTableAndTargetTableList = ExtreamSourceAndExtreamTarget_Util.getExtreamSourceAndTargetTablesListOfSet(mappingSpecifications);

        Set<String> extreameSrcTableSet = null;
        Set<String> extreamTargetSetTables = null;
        try {
            extreameSrcTableSet = extreameSrcTableAndTargetTableList.get(0);

            extreamTargetSetTables = extreameSrcTableAndTargetTableList.get(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {

            for (MappingSpecificationRow mappingSpecificationRow : mappingSpecifications) {

                String sourceTable = mappingSpecificationRow.getSourceTableName();
                String targetTable = mappingSpecificationRow.getTargetTableName();
                if (StringUtils.isNotBlank(sourceTable) && extreameSrcTableSet != null && extreamTargetSetTables != null && !extreameSrcTableSet.contains(sourceTable.toUpperCase()) && !extreamTargetSetTables.contains(sourceTable.toUpperCase())) {
                    mappingSpecificationRow.setSourceSystemEnvironmentName(hirarachyFolderName);
                }
                if (StringUtils.isNotBlank(targetTable) && extreameSrcTableSet != null && extreamTargetSetTables != null && !extreameSrcTableSet.contains(targetTable.toUpperCase()) && !extreamTargetSetTables.contains(targetTable.toUpperCase())) {
                    mappingSpecificationRow.setTargetSystemEnvironmentName(hirarachyFolderName);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
