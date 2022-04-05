/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2014;

import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 26-08-2021
 */
public class BuildDataFlowLineage {

    BuildDataFlowLineage() {
    }

    public static Set<String> removeDuplicates = new HashSet<>();

    /**
     * this method creates lineage for input and output columns for data flow
     * components and prepares a hash map containing column lineage and extended
     * properties
     *
     * @param componentConnections
     * @param executableName
     * @param queryMap
     * @param actualExecutableName
     * @param finalColumnLevelLinageMap
     * @return
     */
    public static Map<String, List<Map<String, String>>> buildMappingLineage(Map<String, String> componentConnections, String executableName, Map<String, String> queryMap, String actualExecutableName, Map<String, List<Map<String, String>>> finalColumnLevelLinageMap) {
        Map<String, String> targetToSourceLineage = new HashMap<>();

        removeDuplicates = new HashSet<>();
        for (Map.Entry<String, String> entrySet : componentConnections.entrySet()) {
            String endId = entrySet.getKey();
            String startId = entrySet.getValue();

            String srcColumnName = "";
            String tgtColumnName = "";
            Set<Map<String, String>> outputset = IterateInputAndOutPutColumns2014.componentColumnIdsForInput.get(endId);
            if (outputset == null) {
                continue;
            }

            for (Map<String, String> outputset1 : outputset) {
                String refId = outputset1.get("refid");
                String dataFlowName = "dataflow";
                try {
                    String[] stringArr = refId.split("\\\\");
                    dataFlowName = stringArr[stringArr.length - 1];
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    if (dataFlowName.equalsIgnoreCase(outputset1.get("dataflowname"))) {
                        String lineageId = outputset1.get("lineageid");
                        try {
                            if (actualExecutableName.contains(",")) {
                                lineageId = lineageId.replace(actualExecutableName, actualExecutableName.replaceAll(",", "_"));
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        if (lineageId.indexOf(".Column") != -1) {
                            startId = lineageId.substring(0, lineageId.indexOf(".Column"));
                        }
                        if (lineageId.contains("[") && lineageId.contains("]")) {
                            srcColumnName = lineageId.substring(lineageId.lastIndexOf("[") + 1, lineageId.lastIndexOf("]"));
                        }
                        try {
                            if (actualExecutableName.contains(",")) {
                                startId = startId.replace(actualExecutableName.replaceAll(",", "_"), actualExecutableName);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        srcColumnName = srcColumnName.replace("@", "");
                        tgtColumnName = outputset1.get("columnName");
                        if (startId.contains(".Outputs") && endId.contains(".Inputs")) {
                            String strId = startId.substring(0, startId.indexOf(".Outputs"));
                            String enId = endId.substring(0, endId.indexOf(".Inputs"));
                            if (strId.equalsIgnoreCase(enId)) {
                                String str = srcColumnName;
                                srcColumnName = tgtColumnName;
                                tgtColumnName = str;
                            }
                        }
                        if (startId.equalsIgnoreCase(endId)) {
                            continue;
                        }

                        int length = lineageId.split(Delimiter.derivedColumnDelimiter).length;
                        if (!lineageId.contains(",")) {
                            targetToSourceLineage = buildComponentColumnLevelLineage(srcColumnName, tgtColumnName, startId, endId, componentConnections, targetToSourceLineage);
                        } else {
                            if (IterateInputAndOutPutColumns2014.componentColumnIdsForOutput.get(lineageId.split(",")[0]) == null && !lineageId.contains(Delimiter.derivedColumnDelimiter)) {
                                targetToSourceLineage = buildComponentColumnLevelLineage(srcColumnName, tgtColumnName, startId, endId, componentConnections, targetToSourceLineage);
                            } else {

                                if (lineageId.contains(Delimiter.derivedColumnDelimiter) && length >= 1) {

                                    lineageId = lineageId.split(Delimiter.derivedColumnDelimiter)[0];
                                }

                                targetToSourceLineage = buildAppendedSourceColumns(lineageId, endId, tgtColumnName, targetToSourceLineage, componentConnections, actualExecutableName);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            List<Map<String, String>> lineageAndExtendedProperties = new ArrayList<>();
            lineageAndExtendedProperties.add(targetToSourceLineage);

            lineageAndExtendedProperties.add(queryMap);

            finalColumnLevelLinageMap.put(executableName, lineageAndExtendedProperties);
        }
        return finalColumnLevelLinageMap;

    }

    /**
     * this method prepares a hash map containing column level lineage
     *
     * @param srcColName
     * @param tgtColName
     * @param source
     * @param target
     * @param componentConnections
     * @param targetToSourceLineage
     * @return
     */
    private static Map<String, String> buildComponentColumnLevelLineage(String srcColName, String tgtColName, String source, String target,
            Map<String, String> componentConnections, Map<String, String> targetToSourceLineage) {
        boolean flag = true;

        while (flag) {
            if (target.contains("\n")) {
                target = target.split("\n")[0];
            }
            String src = componentConnections.get(target);
            if (src == null) {
                src = PrepareDataFlowLineage2014.pathInputsOutputsMap.get(target);
                if (src == null || target.equals(source)) {
                    flag = false;
                    break;
                } else {
                    target = src;
                    src = componentConnections.get(src);
                }
            }
            Set<Map<String, String>> outputset = IterateInputAndOutPutColumns2014.componentColumnIdsForInput.get(target);
            if (outputset == null) {
                continue;
            }
            targetToSourceLineage = buildSourceTargetMap(srcColName, tgtColName, src, target, targetToSourceLineage);
            target = src;
            tgtColName = srcColName;
        }
        return targetToSourceLineage;
    }

    /**
     * this method returns a hash map containing the lineage of source and
     * target components
     *
     * @param lineageId
     * @param endId
     * @param tgtColName
     * @param targetToSourceLineage
     * @param componentConnections
     * @param actualExecutableName
     * @return
     */
    private static Map<String, String> buildAppendedSourceColumns(String lineageId, String endId, String tgtColName, Map<String, String> targetToSourceLineage,
            Map<String, String> componentConnections, String actualExecutableName) {
        String target = "";
        String inputcomponentClassId = "";
        try {
            Set<Map<String, String>> outputset = IterateInputAndOutPutColumns2014.componentColumnIdsForInput.get(endId);
            for (Map<String, String> outputset1 : outputset) {
                String refid = outputset1.get("refid");
                String dataFlow = "";
                try {
                    dataFlow = refid.split("\\.")[0];
                    dataFlow = dataFlow.substring(dataFlow.lastIndexOf("\\") + 1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String outputMapDFName = outputset1.get("dataflowname");
                if (outputMapDFName.contains(dataFlow)) {
                    String colName = outputset1.get("columnName");
                    if (!colName.equals(tgtColName)) {
                        continue;
                    }
                    target = buildColumnLineage(outputset1, "tgt");

                    String src = componentConnections.get(endId);
                    String mainInputComponentName = "";
                    String inputBusinessRule = "";

                    for (Map<String, String> inputset1 : IterateInputAndOutPutColumns2014.componentColumnIdsForOutput.get(src)) {
                        mainInputComponentName = inputset1.get("componentName");
                        try {
                            inputcomponentClassId = inputset1.get("componentClassId");
                            inputBusinessRule = inputset1.get("bussinesRule");

                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        if (!"".equalsIgnoreCase(mainInputComponentName)) {
                            break;
                        }
                    }
                    String source = "";
                    String componentName = "";
                    String columnName = "";
                    String dataType = "";
                    String length = "";
                    String precision = "";
                    String scale = "";
                    String componentClassId = "";
                    String bussinesRule = "";
                    String startId = "";
                    String actualLineageId = lineageId;

                    for (String lid : lineageId.split(",")) {
                        try {
                            if (lid.contains(".C")) {
                                startId = lid.substring(0, lid.lastIndexOf(".C"));
                            } else {
                                startId = lid;
                            }
                            try {
                                if (actualExecutableName.contains(",")) {
                                    actualLineageId = lineageId.replace(actualExecutableName.replaceAll(",", "_"), actualExecutableName);
                                    startId = startId.replace(actualExecutableName.replaceAll(",", "_"), actualExecutableName);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            Set<Map<String, String>> inputset = IterateInputAndOutPutColumns2014.componentColumnIdsForOutput.get(startId);
                            if (inputset == null) {
                                inputset = IterateInputAndOutPutColumns2014.componentColumnIdsForOutput.get(actualLineageId);
                            }
                            if (inputset == null) {
                                continue;
                            }
                            for (Map<String, String> inputset1 : inputset) {
                                String inputSetLineageId = inputset1.get("lineageid");
                                if (lid.equalsIgnoreCase(inputSetLineageId)) {
                                    boolean flag = true;
                                    String clName = inputset1.get("columnName");
                                    for (String name : columnName.split("\n")) {
                                        if (name.equalsIgnoreCase(clName)) {
                                            flag = false;
                                            break;
                                        }
                                    }
                                    if (flag) {
                                        columnName = "".equals(columnName) ? clName : columnName + "\n" + clName;
                                        componentName = "".equals(componentName) ? mainInputComponentName : componentName + "\n" + mainInputComponentName;
                                        dataType = "".equals(dataType) ? inputset1.get("dataType") : dataType + "\n" + inputset1.get("dataType");
                                        length = "".equals(length) ? inputset1.get("length") : length + "\n" + inputset1.get("length");
                                        precision = "".equals(precision) ? inputset1.get("precision") : precision + "\n" + inputset1.get("precision");
                                        scale = "".equals(scale) ? inputset1.get("scale") : scale + "\n" + inputset1.get("scale");
                                        bussinesRule = "".equals(bussinesRule) ? inputset1.get("bussinesRule") : bussinesRule + "\n" + inputset1.get("bussinesRule");
                                        if ("Microsoft.ConditionalSplit".equalsIgnoreCase(inputcomponentClassId)) {
                                            bussinesRule = inputBusinessRule;
                                        }
                                        // Added The  below if condition By Dinesh on June-28 2021 to solve LookUp component Issue(coming as source but it has to come only has middle component)
                                        /* Geha Client
                                        PackageName: DA-XX-31570_CXCOE-MemberIBPCallExtract
                                        DF Name : DF Get Call Data
                                        Issue : Look Up Component coming as source and middle compopnent (actaully it has to come only as a middle component) 
                                         */
                                        if ("Microsoft.Lookup".equalsIgnoreCase(inputcomponentClassId)) {
                                            componentClassId = inputcomponentClassId;
                                        }

                                        break;
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    source = componentName + Delimiter.delimiter + columnName + Delimiter.delimiter + dataType + Delimiter.delimiter + length + Delimiter.delimiter + precision + Delimiter.delimiter + scale + Delimiter.delimiter + componentClassId + Delimiter.delimiter + bussinesRule;
                    targetToSourceLineage.put(target, source);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return targetToSourceLineage;

    }

    /**
     * in this method we are preparing source and target columns lineage map
     * into the HashMap
     *
     * @param srcColumnName
     * @param tgtColumnName
     * @param startId
     * @param target
     * @param targetToSourceLineage
     * @return
     */
    private static Map<String, String> buildSourceTargetMap(String srcColumnName, String tgtColumnName, String startId, String target, Map<String, String> targetToSourceLineage) {
        Set<Map<String, String>> outputset = IterateInputAndOutPutColumns2014.componentColumnIdsForInput.get(target);
        Set<Map<String, String>> inputset = IterateInputAndOutPutColumns2014.componentColumnIdsForOutput.get(startId);

        boolean flag = true;
        String targetDetails = "";
        String sourceDetails = "";
        String inputComponentRefId = "";
        if (StringUtils.isNotBlank(startId) && startId.contains(".")) {
            inputComponentRefId = startId.substring(0, startId.indexOf("."));
        }
        String inputComponentName = PrepareDataFlowLineage2014.componentNamerefIds.get(inputComponentRefId);
        String inputComponentClassId = PrepareDataFlowLineage2014.componentClassIds.get(inputComponentRefId);
        for (Map<String, String> outputset1 : outputset) {
            String tcolName = outputset1.get("columnName");
            if (!tcolName.equalsIgnoreCase(tgtColumnName) || inputset == null) {
                continue;
            }
            flag = true;
            for (Map<String, String> inputset1 : inputset) {
                String newSourceColumn = "";
                boolean newSRCFlag = false;
                try {
                    if (IterateInputAndOutPutColumns2014.externanColumnAndInputColumnMap.get(srcColumnName) != null) {
                        newSourceColumn = IterateInputAndOutPutColumns2014.externanColumnAndInputColumnMap.get(srcColumnName);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String scolName = inputset1.get("columnName");

                if (!scolName.equalsIgnoreCase(srcColumnName)) {

                    if (scolName.equalsIgnoreCase(newSourceColumn)) {
                        srcColumnName = newSourceColumn;
                        newSRCFlag = true;
                    } else {
                        continue;
                    }

                } else {
                    newSRCFlag = true;
                }
                if (newSRCFlag) {
                    flag = false;
                    targetDetails = buildColumnLineage(outputset1, "tgt");
                    if (outputset1.get("bussinesRule") != null && !"NOT_DEFINED".equals(outputset1.get("bussinesRule")) && !"".equals(outputset1.get("bussinesRule"))) {
                        inputset1.put("bussinesRule", outputset1.get("bussinesRule"));
                    }
                    sourceDetails = buildColumnLineage(inputset1, "src");

                    String sourceColumns = targetToSourceLineage.get(targetDetails);
                    if (sourceColumns != null) {
                        if (!isColumnAlreadyExisted(sourceColumns, sourceDetails)) {
                            sourceDetails = sourceColumns + "%%" + sourceDetails;
                        } else {
                            sourceDetails = sourceColumns;
                        }
                    }
                    if (!removeDuplicates.contains(targetDetails + "==>" + sourceDetails)) {
                        targetToSourceLineage.put(targetDetails, sourceDetails);
                        removeDuplicates.add(targetDetails + "==>" + sourceDetails);
                    }

                }

            }
            if (flag) {
                flag = false;
                targetDetails = buildColumnLineage(outputset1, "tgt");
                Map<String, String> inputset1 = buildNewColumn(
                        inputComponentName, inputComponentClassId, srcColumnName, outputset1,
                        PrepareDataFlowLineage2014.pathInputsOutputsMap.get(startId));
                sourceDetails = buildColumnLineage(inputset1, "src");
                String sourceColumns = targetToSourceLineage.get(targetDetails);

                if (sourceColumns != null) {
                    if (!isColumnAlreadyExisted(sourceColumns, sourceDetails)) {
                        sourceDetails = sourceColumns + "%%" + sourceDetails;
                    } else {
                        sourceDetails = sourceColumns;
                    }
                }
                if (!removeDuplicates.contains(targetDetails + "==>" + sourceDetails)) {
                    targetToSourceLineage.put(targetDetails, sourceDetails);
                    removeDuplicates.add(targetDetails + "==>" + sourceDetails);
                }
                if (inputComponentClassId.contains("Microsoft.RowCount")) {
                    targetToSourceLineage.put(targetDetails, sourceDetails);
                }
            }
        }

        return targetToSourceLineage;
    }

    /**
     * this method returns boolean by comparing whether given source and target
     * are same
     *
     * @param value
     * @param source
     * @return
     */
    private static boolean isColumnAlreadyExisted(String value, String source) {
        for (String val : value.split("%%")) {
            if (val.equalsIgnoreCase(source)) {
                return true;
            }
        }
        return false;
    }

    /**
     * this method returns a string containing all the details of a particular
     * column
     *
     * @param set
     * @param sourceOrTarget
     * @return
     */
    private static String buildColumnLineage(Map<String, String> set, String sourceOrTarget) {
        String columnName = "";
        try {
            columnName = set.get("columnName").replace("\"", "");
        } catch (Exception e) {
            e.printStackTrace();
        }

        String str = set.get("componentName") + Delimiter.delimiter + columnName + Delimiter.delimiter
                + set.get("dataType") + Delimiter.delimiter + set.get("length") + Delimiter.delimiter
                + set.get("precision") + Delimiter.delimiter + set.get("scale") + Delimiter.delimiter + set.get("componentClassId") + Delimiter.delimiter + set.get("dataflowname");
        try {
            if (set.get("bussinesRule") != null) {
                str = str + Delimiter.delimiter + set.get("bussinesRule");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return str;
    }

    /**
     * this method prepares a hash map by adding all the column details for a
     * given input values
     *
     * @param refId
     * @param componentName
     * @param classId
     * @param colName
     * @param outputset
     * @param previousId
     * @return
     */
    public static Map<String, String> buildNewColumn(
            String componentName, String classId, String colName, Map<String, String> outputset, String previousId) {
        Map<String, String> map = new LinkedHashMap<>();
        try {
            if (StringUtils.isNotBlank(previousId) && previousId.contains("\n")) {
                previousId = previousId.split("\n")[0];
            }
            Set<Map<String, String>> list = IterateInputAndOutPutColumns2014.componentColumnIdsForInput.get(previousId);
            if (list == null) {
                list = new HashSet<>();
            }
            String refId = previousId + ".Columns[" + colName + "]";

            String dataFlowName = "dataflow";
            try {
                String[] stringArr = refId.split("\\\\");
                dataFlowName = stringArr[stringArr.length - 1];
            } catch (Exception e) {
                e.printStackTrace();
            }
            map.put("refid", refId);
            map.put("lineageid", outputset.get("lineageid"));
            map.put("dataflowname", dataFlowName);
            map.put("componentClassId", classId);
            map.put("componentName", componentName);
            map.put("columnName", colName);
            map.put("dataType", outputset.get("dataType"));
            map.put("length", outputset.get("length"));
            map.put("precision", outputset.get("precision"));
            map.put("scale", outputset.get("scale"));
            list.add(map);
            IterateInputAndOutPutColumns2014.componentColumnIdsForInput.put(previousId, list);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

}
