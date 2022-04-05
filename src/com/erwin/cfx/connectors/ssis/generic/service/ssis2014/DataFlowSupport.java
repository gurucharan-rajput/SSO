/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2014;

import com.erwin.cfx.connectors.ssis.generic.beans.DataFlowBean;
import com.erwin.cfx.connectors.ssis.generic.util.Constants;
import com.erwin.cfx.connectors.ssis.generic.util.DataFlowComponentUtil;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.variablereplacement.DynamicVaribleValueReplacement;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.NodeList;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 24-08-2021
 */
public class DataFlowSupport {

    DataFlowSupport() {
    }

    /**
     * this method returns table,system and environment details for a given
     * connection string
     *
     * @param connectionString
     * @return
     */
    public static String getTableAndSystemAndEnvNameFromConnectionString(String connectionString) {

        String tableName = "";
        String environmentName = "";
        String systemName = "";
        int emmArrayLenght = 0;
        String EMM = "_E_M_M_";
        try {
            emmArrayLenght = connectionString.split(EMM).length;

            if (emmArrayLenght <= 2) {
                tableName = connectionString.split(EMM)[0];
                return tableName;
            } else if (emmArrayLenght <= 3) {
                tableName = connectionString.split(EMM)[0];
                environmentName = connectionString.split(EMM)[2];
                return tableName + Delimiter.delimiter + environmentName;
            } else if (emmArrayLenght <= 4) {
                tableName = connectionString.split(EMM)[0];
                environmentName = connectionString.split(EMM)[2];
                systemName = connectionString.split(EMM)[3];
                return tableName + Delimiter.delimiter + environmentName + Delimiter.delimiter + systemName;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * this method returns updated table name if it contains user,project
     * reference variable replaced with its actual value
     *
     * @param tableName
     * @param packageLevelVariableMap
     * @param projectLevelMap
     * @return
     */
    public static String getTableNameFromVaribleMap(String tableName, Map<String, String> packageLevelVariableMap, Map<String, String> projectLevelMap) {

        boolean userFlag = false;
        boolean projectFlag = false;

        tableName = tableName.replace("[", "").replace("]", "");
        try {

            if (!StringUtils.isBlank(tableName) && tableName.toLowerCase().contains("user::")) {
                userFlag = true;
            } else if (!StringUtils.isBlank(tableName) && tableName.toLowerCase().contains("project::")) {
                projectFlag = true;
            }

            tableName = tableName.split("::")[1];

            try {
                if (tableName.contains(";")) {
                    tableName = tableName.split(";")[0];
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (userFlag && packageLevelVariableMap.get(tableName) != null) {
                tableName = packageLevelVariableMap.get(tableName);
            } else if (projectFlag && projectLevelMap.get(tableName) != null) {
                tableName = projectLevelMap.get(tableName);
            } else {
                tableName = "";
            }
        } catch (Exception e) {
            e.printStackTrace();
            tableName = "";
        }
//        }
        return tableName;
    }

    /**
     * this method prepares a hash map mapping output component Id to input Id
     *
     * @param inRefList
     * @param outRefList
     * @param pathInputsOutputsMap
     */
    public static void prepareComponenInputOutputsPath(List<String> inRefList, List<String> outRefList, Map<String, String> pathInputsOutputsMap) {
        Object[] obj = inRefList.size() < outRefList.size() ? outRefList.toArray() : inRefList.toArray();
        for (int i = 0; i < obj.length; i++) {
            String output = "";
            String input = "";
            if (inRefList.toArray().length == 0 || outRefList.toArray().length == 0) {
                continue;
            }
            if (i >= inRefList.toArray().length) {
                input = inRefList.toArray()[inRefList.toArray().length - 1].toString();
            } else {
                input = inRefList.toArray()[i].toString();
            }
            if (i >= outRefList.toArray().length) {
                input = outRefList.toArray()[outRefList.toArray().length - 1].toString();
            } else {
                output = outRefList.toArray()[i].toString();
            }

            if (pathInputsOutputsMap.get(output) != null) {
                input = pathInputsOutputsMap.get(output) + "\n" + input;
            }
            pathInputsOutputsMap.put(output, input);
        }
    }

    /**
     * this method returns table name based on the access mode
     *
     * @param accessMode
     * @param xmlData
     * @param xmlDatavariable
     * @return
     */
    public static String getTheTableNameBasedOnTheXMLAccessMode(String accessMode, String xmlData, String xmlDatavariable) {
        String tableName = "";
        if ("0".equalsIgnoreCase(accessMode)) {
            tableName = xmlData;
        } else {
            tableName = xmlDatavariable;
        }

        return tableName;
    }

    /**
     * in this method we are updating columns set with component name,DataFlow
     * name and componentClassId providing these 3 are mandatory
     *
     * @param list
     * @param componentName
     * @param componentClassId
     * @return updatedColumnSet
     */
    public static Set<LinkedHashMap<String, String>> updateDataFlowNameInColumnSet(Set<LinkedHashMap<String, String>> list, String componentName, String componentClassId) {
        Set<LinkedHashMap<String, String>> returnColumnSet = new HashSet();
        try {

            String dataFlowName = "datatflowname";

            for (LinkedHashMap<String, String> linkedHashMap : list) {
                try {
                    String inputColumnRefId = linkedHashMap.get("lineageid");
                    String[] stringArr = inputColumnRefId.split("\\\\");
                    dataFlowName = stringArr[stringArr.length - 1];
                } catch (Exception e) {
                    e.printStackTrace();
                }
                LinkedHashMap<String, String> newMap = new LinkedHashMap(linkedHashMap);
                newMap.put("dataflowname", dataFlowName);
                newMap.put("componentName", componentName);
                newMap.put("componentClassId", componentClassId);
                returnColumnSet.add(newMap);
            }
        } catch (Exception e) {

        }
        return returnColumnSet;
    }

    public static void getFileNameDetailsFromForEachLoop(String forEachLoopFileName, DataFlowBean dataFlowBean) {
        String userOrProjectName = "";
        String userOrProjectReference = "";
        try {
            if (StringUtils.isNotBlank(forEachLoopFileName) && forEachLoopFileName.split(Delimiter.delimiter).length >= 2) {
                userOrProjectName = forEachLoopFileName.split(Delimiter.delimiter)[0];
                forEachLoopFileName = forEachLoopFileName.split(Delimiter.delimiter)[1];
                if (!org.apache.commons.lang.StringUtils.isBlank(forEachLoopFileName)) {
                    forEachLoopFileName = forEachLoopFileName.trim();
                    if (forEachLoopFileName.startsWith("*.")) {
                        forEachLoopFileName = "";
                    }
                }

                if (StringUtils.isNotBlank(userOrProjectName)) {
                    userOrProjectName = DynamicVaribleValueReplacement.returnVariableOrProjectNameWithSplitingUserOrProjectReference(userOrProjectName);

                    if (StringUtils.isNotBlank(userOrProjectName) && userOrProjectName.split(Delimiter.delimiter).length >= 2) {
                        userOrProjectReference = userOrProjectName.split(Delimiter.delimiter)[0];
                        userOrProjectName = userOrProjectName.split(Delimiter.delimiter)[1];

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataFlowBean.setFileNameFromForEachLoop(forEachLoopFileName);
        dataFlowBean.setUserOrProjectName(userOrProjectName);
        dataFlowBean.setUserOrProjectReference(userOrProjectReference);
    }

    public static String returnTableNameOrQueryFromPropertiesFile(NodeList property, DataFlowBean dataFlowBean, String componentDecision, String componentClassId) {

        String connectionRefId = "";
        String tableName = "";
        String keyTableName = "";
        String sourceFilePath = "";
        String xmlFilePath = "";
        String openRowsetTableName = "";

        String OpenRowsetVariableName = "";
        String xmlDataVariable = "";
        String xmlData = "";
        String propertyTableName = "";
        String tableOrViewName = "";
        String bulkInsertTableName = "";
        String fileDataBaseType = "";
        String storeProcName = "";
        String query = "";
        String queryFromVarible = "";

        String sheetName = "";
        String destinationFilePath = "";
        String destinationTable = "";
        String accessmode = "";
        boolean isFileComponentFlag = false;
        String joinTypeForMergeComponent = "";
        String joinTypeNameForMergeComponent = "";
        String dfUtilDelimiter = Delimiter.dataFlowUtilDelimiter;
        String rawSourceFileName = "";
        String rawSourceVariableFileName = "";
        String collectionNameForODataSource = "";
        String fileExtension = "";
        String destinationTableName = "";
        String destinationTableSchema = "";
        String changedDelimiter = "@e_cr_u_os@";
        Map<String, String> packageLevelMap = dataFlowBean.getSsisInputParameterBean().getUserReferenceVariablesMap();
        Map<String, String> projectLevelMap = dataFlowBean.getAcpInputParameterBean().getProjectLevelVaraiblesMap();

        for (int l = 0; l < property.getLength(); l++) {
            if ("property".equalsIgnoreCase(property.item(l).getNodeName())) {
                if ("OpenRowset".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    openRowsetTableName = property.item(l).getTextContent();

//                    if (!StringUtils.isBlank(openRowsetTableName)) {
//                        sheetName = openRowsetTableName;
//                        tableName = openRowsetTableName;
//
//                    }
//                    if (!StringUtils.isBlank(sheetName) && sheetName.contains("$")) {
//                        isFileComponentFlag = true;
//                        fileDataBaseType = "CSV";
//                    }
                }
                if ("SourceFilePath".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    sourceFilePath = property.item(l).getTextContent();
                    sourceFilePath = FilenameUtils.normalizeNoEndSeparator(sourceFilePath, true);

                    tableName = DataFlowComponentUtil.returnTableNameFromFilePath(sourceFilePath);
                    try {
                        if (tableName.contains(Delimiter.fileExtensionDelimiter)) {
                            fileExtension = tableName.split(Delimiter.fileExtensionDelimiter)[1];
                            tableName = tableName.split(Delimiter.fileExtensionDelimiter)[0];
                        }

                        fileDataBaseType = DataFlowComponentUtil.getTheFileDatabaseTypeBasedonTheFileExtension(fileExtension, dataFlowBean);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (StringUtils.isNotBlank(fileDataBaseType)) {
                        isFileComponentFlag = true;
                    }

                    if (!StringUtils.isBlank(sheetName) && StringUtils.isBlank(tableName)) {
                        tableName = sheetName;
                    } else if (!StringUtils.isBlank(sheetName) && !StringUtils.isBlank(tableName)) {
                        tableName = tableName + "." + sheetName;
                    }

                }

                // added the below if condition on aug-2 2021 for getting tableName from properties tags when it is from odata source
                if ("CollectionName".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    collectionNameForODataSource = property.item(l).getTextContent();
                }
                if ("DestinationTableName".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    destinationTableName = property.item(l).getTextContent();
                    tableName = destinationTableName;
                }
                if ("DestinationTableSchema".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    destinationTableSchema = property.item(l).getTextContent();

                    if (StringUtils.isNotBlank(destinationTableName) && StringUtils.isNotBlank(destinationTableSchema)) {
                        tableName = destinationTableSchema + "." + destinationTableName;
                    } else if (StringUtils.isNotBlank(destinationTableName)) {
                        tableName = destinationTableName;
                    }
                }
                if ("OpenRowsetVariable".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    OpenRowsetVariableName = property.item(l).getTextContent();

                    OpenRowsetVariableName = getVaribleValueFromOpenRowsetVariable(OpenRowsetVariableName, packageLevelMap, projectLevelMap);

//                    if (!StringUtils.isBlank(OpenRowsetVariableName)) {
//                        sheetName = OpenRowsetVariableName;
//                        tableName = OpenRowsetVariableName;
//                    }
//                    if (!StringUtils.isBlank(sheetName) && sheetName.contains("$")) {
//                        isFileComponentFlag = true;
//                        fileDataBaseType = "CSV";
//                    }
                }
                if ("XMLDataVariable".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    xmlDataVariable = property.item(l).getTextContent();

                    xmlDataVariable = getVaribleValueFromOpenRowsetVariable(xmlDataVariable, packageLevelMap, projectLevelMap);

                    xmlDataVariable = FilenameUtils.normalizeNoEndSeparator(xmlDataVariable, true);
                    xmlDataVariable = DataFlowComponentUtil.returnTableNameFromFilePath(xmlDataVariable);
                    try {
                        if (xmlDataVariable.contains(Delimiter.fileExtensionDelimiter)) {
                            fileExtension = xmlDataVariable.split(Delimiter.fileExtensionDelimiter)[1];
                            xmlDataVariable = xmlDataVariable.split(Delimiter.fileExtensionDelimiter)[0];
                        }
                        fileDataBaseType = DataFlowComponentUtil.getTheFileDatabaseTypeBasedonTheFileExtension(fileExtension, dataFlowBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (StringUtils.isNotBlank(fileDataBaseType)) {
                        isFileComponentFlag = true;
                    }

                }

                if ("TableOrViewName".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    tableOrViewName = property.item(l).getTextContent();
                    if (tableOrViewName != null && !StringUtils.isBlank(tableOrViewName.trim())) {
                        tableName = tableOrViewName;
                    }

                }
                if ("WorkSheetName".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    sheetName = property.item(l).getTextContent();

                }
                if ("FileName".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    rawSourceFileName = property.item(l).getTextContent();
                    rawSourceFileName = FilenameUtils.normalizeNoEndSeparator(rawSourceFileName, true);

                }
                if ("FileNameVariable".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    rawSourceVariableFileName = property.item(l).getTextContent();
                    rawSourceVariableFileName = FilenameUtils.normalizeNoEndSeparator(rawSourceVariableFileName, true);

                }
                if ("JoinType".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    joinTypeForMergeComponent = property.item(l).getTextContent();

                }
                if ("DestinationFilePath".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    destinationFilePath = property.item(l).getTextContent();
                    destinationFilePath = FilenameUtils.normalizeNoEndSeparator(destinationFilePath, true);
                    tableName = DataFlowComponentUtil.returnTableNameFromFilePath(destinationFilePath);
                    try {
                        if (tableName.contains(Delimiter.fileExtensionDelimiter)) {
                            fileExtension = tableName.split(Delimiter.fileExtensionDelimiter)[1];
                            tableName = tableName.split(Delimiter.fileExtensionDelimiter)[0];
                        }
                        fileDataBaseType = DataFlowComponentUtil.getTheFileDatabaseTypeBasedonTheFileExtension(fileExtension, dataFlowBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (StringUtils.isNotBlank(fileDataBaseType)) {
                        isFileComponentFlag = true;
                    }

                    if (!StringUtils.isBlank(tableName) && !StringUtils.isBlank(sheetName)) {
                        tableName = tableName + "." + sheetName;
                    } else if (!StringUtils.isBlank(sheetName)) {
                        tableName = sheetName;
                    }
                }
                if ("DestinationTable".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    destinationTable = property.item(l).getTextContent();
                    tableName = destinationTable;

                }

                // added the below if condition on nov-6 2020 for getting tableName from properties tags when it is from odbc source or taget
                if ("TableName".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    propertyTableName = property.item(l).getTextContent();
                    if (propertyTableName != null && !StringUtils.isBlank(propertyTableName.trim())) {
                        tableName = propertyTableName;
                    }

                }
                // added the below if condition on nov-6 2020 for getting tableName from properties tags when it is from sqlServer  destination
                if ("BulkInsertTableName".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    bulkInsertTableName = property.item(l).getTextContent();
                    if (bulkInsertTableName != null && !StringUtils.isBlank(bulkInsertTableName.trim())) {
                        tableName = bulkInsertTableName;
                    }

                }

                if ("DESTINATION_TABLE_NAME".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    destinationTableName = property.item(l).getTextContent();
                    if (destinationTableName != null && !StringUtils.isBlank(destinationTableName.trim())) {
                        tableName = destinationTableName;
                    }

                }
                if ("SqlCommand".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {

                    //Query 
                    query = property.item(l).getTextContent();

                } else if (property.item(l) != null && "XMLData".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    xmlData = property.item(l).getTextContent();
                    xmlData = FilenameUtils.normalizeNoEndSeparator(xmlData, true);

                    xmlData = DataFlowComponentUtil.returnTableNameFromFilePath(xmlData);
                    try {
                        if (xmlData.contains(Delimiter.fileExtensionDelimiter)) {
                            fileExtension = xmlData.split(Delimiter.fileExtensionDelimiter)[1];
                            xmlData = xmlData.split(Delimiter.fileExtensionDelimiter)[0];
                        }
                        fileDataBaseType = DataFlowComponentUtil.getTheFileDatabaseTypeBasedonTheFileExtension(fileExtension, dataFlowBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (StringUtils.isNotBlank(fileDataBaseType)) {
                        isFileComponentFlag = true;
                    }

                } else if ("SqlCommandVariable".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {

                    String sqlvariable = property.item(l).getTextContent();
                    try {
                        if (sqlvariable.contains("User::")) {

                            sqlvariable = sqlvariable.replace("User::", "");

                            if (packageLevelMap.get(sqlvariable) != null) {
                                queryFromVarible = packageLevelMap.get(sqlvariable);
                            }

                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                    }
                }
                try {
                    if ("AccessMode".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                        accessmode = property.item(l).getTextContent();

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
        tableName = DataFlowComponentUtil.getTableNameBasedOnAccesMode(openRowsetTableName, OpenRowsetVariableName, accessmode, componentDecision, tableName);
        if (StringUtils.isNotBlank(tableName) && tableName.endsWith(changedDelimiter)) {
            tableName = tableName.split(changedDelimiter)[0];
            sheetName = tableName;
            if (!StringUtils.isBlank(sheetName) && sheetName.contains("$")) {
                isFileComponentFlag = true;
                fileDataBaseType = "CSV";
            }
        }
        dataFlowBean.setSheetName(sheetName);
        dataFlowBean.setAccessMode(accessmode);
        dataFlowBean.setCollectionNameForODataSource(collectionNameForODataSource);
        dataFlowBean.setFileExtension(fileExtension);
        dataFlowBean.setIsFileComponent(isFileComponentFlag);
        dataFlowBean.setFileDataBaseType(fileDataBaseType);
        dataFlowBean.setXmlData(xmlData);
        dataFlowBean.setRawSourceVariableFileName(rawSourceVariableFileName);
        dataFlowBean.setRawSourceFileName(rawSourceFileName);
        dataFlowBean.setXmlDataVariable(xmlDataVariable);

        query = DataFlowComponentUtil.gettingQueryBasedOnAccessMode(query, queryFromVarible, accessmode);
        tableName = DataFlowComponentUtil.makeTableNameAsEmptyBasedOnAccessMode(accessmode, componentDecision, tableName, componentClassId);
        query = DataFlowComponentUtil.makeQueryAsEmptyBasedOnAccessMode(accessmode, componentDecision, query, componentClassId);

        if (StringUtils.isNotBlank(query) && StringUtils.isBlank(tableName)) {
            return query + Constants.queryConstant;
        } else if (StringUtils.isNotBlank(tableName) && StringUtils.isBlank(query)) {
            return tableName + Constants.tableConstant;
        } else {
            return "";
        }

    }

    public static String getVaribleValueFromOpenRowsetVariable(String varibleName, Map<String, String> packageLevelMap, Map<String, String> projectLevelMap) {
        String varibleValue = "";

        try {
            if (varibleName.contains("User::")) {
                varibleName = varibleName.split("User::")[1];
                if (packageLevelMap.get(varibleName) != null) {
                    varibleValue = packageLevelMap.get(varibleName);
                }
            }
            if (varibleName.contains("Project::")) {
                varibleName = varibleName.split("Project::")[1];
                if (projectLevelMap.get(varibleName) != null) {
                    varibleValue = projectLevelMap.get(varibleName);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return varibleValue;

    }

}
