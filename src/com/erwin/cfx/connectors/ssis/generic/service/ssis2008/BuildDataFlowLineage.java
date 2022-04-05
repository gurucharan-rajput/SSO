/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2008;

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
 * @author THarika
 */
public class BuildDataFlowLineage {

    /**
     * this method creates lineage for input and output columns for data flow
     * components and prepares a hash map containing column lineage and extended
     * properties
     *
     * @param componentConnections
     * @param executableName
     * @param queryMap
     * @return
     */
    public static Map<String, List<Map<String, String>>> createMapping(Map<String, String> componentConnections, String executableName, HashMap<String, String> queryMap) {

        Map<String, String> targetToSourceLineage = new HashMap<>();
        PrepareDataFlowLineage2008.removeDuplicates = new HashSet<>();
        long startTime = System.currentTimeMillis(); //createMapping

        for (Map.Entry<String, String> entrySet : componentConnections.entrySet()) {
            String endId = entrySet.getKey();
            String startId = entrySet.getValue();
            String srcColumnName = "";
            String tgtColumnName = "";
            Set<Map<String, String>> outputset = PrepareDataFlowLineage2008.componentColumnIdsForInput.get(endId);
            PrepareDataFlowLineage2008.componentColumnIdsForInput.toString();
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
                        if (StringUtils.isNotBlank(lineageId) && lineageId.endsWith(")")) {
                            lineageId = lineageId.substring(0, lineageId.lastIndexOf(")"));
                        }
                        if (lineageId.indexOf(".Column") != -1) {
                            startId = lineageId.substring(0, lineageId.indexOf(".Column"));
                        }

                        try {
                            if (PrepareDataFlowLineage2008.columnRefIdAndTotalColumnDetailsMap.get(lineageId) != null) {
                                srcColumnName = PrepareDataFlowLineage2008.columnRefIdAndTotalColumnDetailsMap.get(lineageId).split("!@ERWIN@!")[5];
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        tgtColumnName = outputset1.get("columnName");

                        if (StringUtils.isBlank(tgtColumnName)) {
                            continue;
                        }
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
                        if (!lineageId.contains(",")) {
                            targetToSourceLineage = buildComponentColumnLevelLineage(srcColumnName, tgtColumnName, startId, endId, componentConnections, targetToSourceLineage);
                        } else {
                            if (PrepareDataFlowLineage2008.componentColumnIdsForOutput.get(lineageId.split(",")[0]) == null) {
                                targetToSourceLineage = buildComponentColumnLevelLineage(srcColumnName, tgtColumnName, startId, endId, componentConnections, targetToSourceLineage);
                            } else {
                                targetToSourceLineage = buildAppendedSourceColumns(lineageId, endId, tgtColumnName, targetToSourceLineage, componentConnections);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            List<Map<String, String>> lineageAndExtendedProperties = new ArrayList<>();
            lineageAndExtendedProperties.add(targetToSourceLineage);

            queryMap.put("variables", PrepareDataFlowLineage2008.variablesString);
            lineageAndExtendedProperties.add(queryMap);

            PrepareDataFlowLineage2008.finalColumnLevelLinageMap.put(executableName, lineageAndExtendedProperties);
        }

        long endTime = System.currentTimeMillis(); //createMapping
        System.out.println("time difference----in createMapping method::" + (endTime - startTime));
        return PrepareDataFlowLineage2008.finalColumnLevelLinageMap;

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
        long startTime = System.currentTimeMillis();//buildComponentColumnLevelLineage

        while (flag) {
            String src = componentConnections.get(target);
            if (src == null) {
                src = PrepareDataFlowLineage2008.pathInputsOutputsMap.get(target);
                if (src == null || target.equals(source)) {
                    flag = false;
                    break;
                } else {
                    target = src;
                    src = componentConnections.get(src);
                }
            }
            Set<Map<String, String>> outputset = PrepareDataFlowLineage2008.componentColumnIdsForInput.get(target);
            if (outputset == null) {
                continue;
            }
            targetToSourceLineage = buildSourceTargetMap(srcColName, tgtColName, src, target, targetToSourceLineage);
            target = src;
            tgtColName = srcColName;
        }
        long endTime = System.currentTimeMillis();//buildComponentColumnLevelLineage
        System.out.println("time difference----in buildComponentColumnLevelLineage method::" + (endTime - startTime));
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

        long startTime = System.currentTimeMillis();//buildSourceTargetMap
        PrepareDataFlowLineage2008.componentColumnIdsForInput.toString();

        Set<Map<String, String>> outputset = PrepareDataFlowLineage2008.componentColumnIdsForInput.get(target);
        Set<Map<String, String>> inputset = PrepareDataFlowLineage2008.componentColumnIdsForOutput.get(startId);
        boolean flag = true;
        String targetDetails = "";
        String sourceDetails = "";
        String inputComponentRefId = "";
        try {
            inputComponentRefId = PrepareDataFlowLineage2008.startOrEndIdComponentIdHM.get(startId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String inputComponentName = PrepareDataFlowLineage2008.componentNamerefIds.get(inputComponentRefId);
        String inputComponentClassId = PrepareDataFlowLineage2008.componentClassIds.get(inputComponentRefId);
        for (Map<String, String> outputset1 : outputset) {
            String tcolName = outputset1.get("columnName");
            if (!tcolName.equalsIgnoreCase(tgtColumnName)) {
                continue;
            }
            for (Map<String, String> inputset1 : inputset) {
                String scolName = "";

                if (!inputset1.isEmpty()) {
                    scolName = inputset1.get("columnName");
                }

                if (inputComponentClassId.contains("Microsoft.RowCount") && !tcolName.contains("Row Count Output")) {
                    int a = 2;
                } else if (!scolName.equalsIgnoreCase(srcColumnName)) {
                    continue;
                }
                flag = false;
                targetDetails = buildMap(outputset1);
                sourceDetails = buildMap(inputset1);
                String sourceColumns = targetToSourceLineage.get(targetDetails);
                if (sourceColumns != null) {
                    if (!isColumnAlreadyExisted(sourceColumns, sourceDetails)) {
                        sourceDetails = sourceColumns + "%%" + sourceDetails;
                    } else {
                        sourceDetails = sourceColumns;
                    }
                }
                if (!PrepareDataFlowLineage2008.removeDuplicates.contains(targetDetails + "==>" + sourceDetails)) {
                    targetToSourceLineage.put(targetDetails, sourceDetails);
                    PrepareDataFlowLineage2008.removeDuplicates.add(targetDetails + "==>" + sourceDetails);
                }
            }
            if (flag) {
                flag = false;
                targetDetails = buildMap(outputset1);
                Map<String, String> inputset1 = buildNewColumn(inputComponentRefId,
                        inputComponentName, inputComponentClassId, srcColumnName, outputset1,
                        PrepareDataFlowLineage2008.pathInputsOutputsMap.get(startId));
                sourceDetails = buildMap(inputset1);
                String sourceColumns = targetToSourceLineage.get(targetDetails);

                if (sourceColumns != null) {
                    if (!isColumnAlreadyExisted(sourceColumns, sourceDetails) && !sourceDetails.contains("Microsoft.ConditionalSplit")) {
                        sourceDetails = sourceColumns + "%%" + sourceDetails;
                    } else {
                        sourceDetails = sourceColumns;
                    }
                }
                if (!PrepareDataFlowLineage2008.removeDuplicates.contains(targetDetails + "==>" + sourceDetails)) {
                    targetToSourceLineage.put(targetDetails, sourceDetails);
                    PrepareDataFlowLineage2008.removeDuplicates.add(targetDetails + "==>" + sourceDetails);
                }
                if (inputComponentClassId.contains("Microsoft.RowCount")) {
                    targetToSourceLineage.put(targetDetails, sourceDetails);
                }
            }
        }
        long endTime = System.currentTimeMillis();//buildSourceTargetMap
        System.out.println("time difference----in buildSourceTargetMap method::" + (endTime - startTime));
        return targetToSourceLineage;
    }

    /**
     * this method returns a string containing all the details of a particular
     * column
     *
     * @param set
     * @return
     */
    private static String buildMap(Map<String, String> set) {
        long startTime = System.currentTimeMillis();
        String str = set.get("componentName") + Delimiter.delimiter + set.get("columnName") + Delimiter.delimiter
                + set.get("dataType") + Delimiter.delimiter + set.get("length") + Delimiter.delimiter
                + set.get("precision") + Delimiter.delimiter + set.get("scale") + Delimiter.delimiter + set.get("componentClassId") + Delimiter.delimiter + set.get("dataflowname");
        if (set.get("bussinessRule") != null) {
            str = str + Delimiter.delimiter + set.get("bussinessRule");
        }
        long endTime = System.currentTimeMillis();//buildSourceTargetMap
        System.out.println("time difference----in buildSourceTargetMap method::" + (endTime - startTime));
        return str;
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
    private static Map<String, String> buildNewColumn(String refId,
            String componentName, String classId, String colName, Map<String, String> outputset, String previousId) {
        if (previousId.contains("\n")) {
            previousId = previousId.split("\n")[0];
        }
        Set<Map<String, String>> list = PrepareDataFlowLineage2008.componentColumnIdsForInput.get(previousId);
        long startTime = System.currentTimeMillis();//buildNewColumn

        if (list == null) {
            list = new HashSet<>();
        }
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
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
        PrepareDataFlowLineage2008.componentColumnIdsForInput.put(previousId, list);
        long endTime = System.currentTimeMillis();//buildNewColumn
        System.out.println("time difference----in buildNewColumn method::" + (endTime - startTime));
        return map;
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
     * @return
     */
    private static Map<String, String> buildAppendedSourceColumns(String lineageId, String endId, String tgtColName, Map<String, String> targetToSourceLineage,
            Map<String, String> componentConnections) {
        String target = "";
        long startTime = System.currentTimeMillis();//buildAppendedSourceColumns;
        Set<Map<String, String>> outputset = PrepareDataFlowLineage2008.componentColumnIdsForInput.get(endId);
        for (Map<String, String> outputset1 : outputset) {
            String refid = outputset1.get("refid");
            String dataFlow = refid.split("\\\\")[4];
            if (dataFlow.equalsIgnoreCase(outputset1.get("dataflowname"))) {
                String colName = outputset1.get("columnName");
                if (!colName.equals(tgtColName)) {
                    continue;
                }
                target = buildMap(outputset1);

                String src = componentConnections.get(endId);
                String mainInputComponentName = "";
                PrepareDataFlowLineage2008.componentColumnIdsForOutput.get(src);
                for (Map<String, String> inputset1 : PrepareDataFlowLineage2008.componentColumnIdsForOutput.get(src)) {
                    mainInputComponentName = inputset1.get("componentName");
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
                HashSet<String> uniqueSet = new HashSet<>();

                for (String lid : lineageId.split(",")) {
                    try {
                        if (lid.contains(".C")) {
                            startId = lid.substring(0, lid.lastIndexOf(".C"));
                        } else {
                            startId = lid;
                        }

                        Set<Map<String, String>> inputset = PrepareDataFlowLineage2008.componentColumnIdsForOutput.get(startId);
                        if (inputset == null) {
                            inputset = PrepareDataFlowLineage2008.componentColumnIdsForOutput.get(lineageId);
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
                                    bussinesRule = "".equals(bussinesRule) ? inputset1.get("bussinessRule") : bussinesRule + "\n" + inputset1.get("bussinessRule");
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                source = componentName + "#" + columnName + "#" + dataType + "#" + length + "#" + precision + "#" + scale + "#" + componentClassId + "#" + bussinesRule;
                targetToSourceLineage.put(target, source);
            }
        }
        long endTime = System.currentTimeMillis();//buildAppendedSourceColumns
        System.out.println("time difference----in buildAppendedSourceColumns method::" + (endTime - startTime));
        return targetToSourceLineage;
    }
}
