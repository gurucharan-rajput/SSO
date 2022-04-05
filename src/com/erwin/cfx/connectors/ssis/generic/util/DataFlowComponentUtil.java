/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.beans.DataFlowBean;
import com.erwin.cfx.connectors.ssis.generic.service.ssis2014.DataFlowSupport;
import com.erwin.cfx.connectors.ssis.generic.util.variablereplacement.DynamicVaribleValueReplacement;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date :- 19-08-2021
 */
public class DataFlowComponentUtil {

    /**
     * in this method we are taking connection string as input and and returning
     * the table(file name) information back to the calling method.
     *
     * @param connectionString
     * @param defaultConnectionString
     * @param dataFlowBean
     * @return table Info
     */
    public static String getTableNameAndDatabaseTypeFromTheConnectioString(String connectionString, String defaultConnectionString, DataFlowBean dataFlowBean) {
        String getDateReplacementFlag = "false";
        String fileExtension = "";

        String tableName = "";
        String fileDataBaseType = "";
        String sheetName = "";
        boolean isFileComponentFlag = false;
        Map<String, String> packageLevelMap = dataFlowBean.getSsisInputParameterBean().getUserReferenceVariablesMap();
        Map<String, String> projectLevelMap = dataFlowBean.getAcpInputParameterBean().getProjectLevelVaraiblesMap();
        try {
            String userOrProjectName = dataFlowBean.getUserOrProjectName();
            String userOrProjectReference = dataFlowBean.getUserOrProjectReference();
            String fileSpecFromForEachLoopContainer = dataFlowBean.getFileNameFromForEachLoop();
            connectionString = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbols(connectionString, userOrProjectName, fileSpecFromForEachLoopContainer, userOrProjectReference, "fileTypeComponent");

            String[] connectionStringArray = connectionString.split(Delimiter.getDateReplacementDelimiter);
            int connectionStringLength = connectionStringArray.length;
            switch (connectionStringLength) {
                case 2:
                    getDateReplacementFlag = connectionStringArray[1];
                    connectionString = connectionStringArray[0];
                    break;
                case 1:
                    connectionString = connectionStringArray[0];
                    break;
                default:
                    break;
            }

            sheetName = dataFlowBean.getSheetName();

        } catch (Exception e) {
            e.printStackTrace();
        }
        String dummyData = "test";

        String tableAndEnvAndSystem = getTableFilePathFromConnectionString(connectionString);
        try {

            tableName = tableAndEnvAndSystem.split(Delimiter.delimiter)[2];
        } catch (Exception e) {
            e.printStackTrace();
        }

        tableName = returnTableNameFromFilePath(tableName);

        try {
            if (tableName.contains("@") && tableName.contains("::")) {
                tableName = DataFlowSupport.getTableNameFromVaribleMap(tableName, packageLevelMap, projectLevelMap);

                tableName = returnTableNameFromFilePath(tableName);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (tableName.contains(Delimiter.fileExtensionDelimiter)) {
                fileExtension = tableName.split(Delimiter.fileExtensionDelimiter)[1];
                tableName = tableName.split(Delimiter.fileExtensionDelimiter)[0];
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        String delimiter = Delimiter.dataFlowUtilDelimiter;

        if ((!StringUtils.isBlank(tableName) && (tableName.contains("(") || tableName.contains(")"))) || StringUtils.isBlank(tableName)) {

            String defaultConnectionStringTableAndEnvAndSystem = getTableFilePathFromConnectionString(defaultConnectionString);
            try {

                tableName = defaultConnectionStringTableAndEnvAndSystem.split(Delimiter.delimiter)[2];
            } catch (Exception e) {
                e.printStackTrace();
            }

            tableName = returnTableNameFromFilePath(tableName);
            try {
                if (tableName.contains(Delimiter.fileExtensionDelimiter)) {
                    fileExtension = tableName.split(Delimiter.fileExtensionDelimiter)[1];
                    tableName = tableName.split(Delimiter.fileExtensionDelimiter)[0];
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        try {
            tableName = tableName.replace("\"", "").replace("+", "").trim();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (!StringUtils.isBlank(tableName)) {
                tableName = tableName.trim();
//                if (tableName.endsWith("*")) {
//                    tableName = tableName.substring(0, tableName.lastIndexOf("*"));
//                }
//                if (tableName.endsWith("_")) {
//                    tableName = tableName.substring(0, tableName.lastIndexOf("_"));
//                }
                tableName = replaceSpecialCharaterFromEnd(tableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (fileExtension.equalsIgnoreCase("*")) {
            fileExtension = "csv";
            fileDataBaseType = "DSV";
            isFileComponentFlag = true;
        }

        try {
            if (!StringUtils.isBlank(tableName)) {
                tableName = tableName.replace("\n", "").replaceAll("\\s+", " ");
                tableName = tableName.replaceAll("\\s+_", "_").replaceAll("_\\s+", "_");
                tableName = tableName.replaceAll("\\s+\\.", ".").replaceAll("\\.\\s+", ".");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            fileDataBaseType = getTheFileDatabaseTypeBasedonTheFileExtension(fileExtension, dataFlowBean);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!StringUtils.isBlank(fileDataBaseType) && !StringUtils.isBlank(tableName)) {
            isFileComponentFlag = true;
        }

        if (!StringUtils.isBlank(sheetName) && !StringUtils.isBlank(tableName)) {
            tableName = tableName + "." + sheetName;
        } else if (!StringUtils.isBlank(sheetName)) {
            tableName = sheetName;
            fileDataBaseType = "CSV";
            isFileComponentFlag = true;
        }

        return tableName + delimiter + fileDataBaseType + delimiter
                + isFileComponentFlag + delimiter + fileExtension + delimiter + dummyData;
    }

    /**
     * this method returns the file path for a given connection string
     *
     * @param connectionString
     * @return
     */
    public static String getTableFilePathFromConnectionString(String connectionString) {
        String tableName = "";
        String systemName = "";
        String environmentName = "";

        try {
            String tabEnvAndSystemName = "";

            tabEnvAndSystemName = DataFlowSupport.getTableAndSystemAndEnvNameFromConnectionString(connectionString);
            int length = tabEnvAndSystemName.split(Delimiter.delimiter).length;
            switch (length) {
                case 1:
                    tableName = tabEnvAndSystemName.split(Delimiter.delimiter)[0];
                    break;
                case 2:
                    tableName = tabEnvAndSystemName.split(Delimiter.delimiter)[0];
                    environmentName = tabEnvAndSystemName.split(Delimiter.delimiter)[1];
                    break;
                case 3:
                    tableName = tabEnvAndSystemName.split(Delimiter.delimiter)[0];
                    environmentName = tabEnvAndSystemName.split(Delimiter.delimiter)[1];
                    systemName = tabEnvAndSystemName.split(Delimiter.delimiter)[2];
                    break;
                default:
                    break;

            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getTableFilePathFromConnectionString(-,-)");
        }
        String dummyTest = "test";

        return environmentName + Delimiter.delimiter + systemName + Delimiter.delimiter + tableName + Delimiter.delimiter + dummyTest;
    }

    /**
     * this method returns the file database type based on the file extension
     *
     * @param fileExtension
     * @param dataFlowBean
     * @return
     */
    public static String getTheFileDatabaseTypeBasedonTheFileExtension(String fileExtension, DataFlowBean dataFlowBean) {
        String fileDataBaseType = "";

        List<String> fileExtensionList = null;
        if (dataFlowBean != null) {
            ACPInputParameterBean acpInputParameterBean = dataFlowBean.getAcpInputParameterBean();
            if (acpInputParameterBean != null) {
                fileExtensionList = acpInputParameterBean.getFileExtensionList();
            }

        }
        try {

            if (StringUtils.isNotBlank(fileExtension)) {
                fileExtension = fileExtension.trim().toLowerCase().replace(";", "").replace("\"", "").replace("\'", "");

            }
            if (fileExtensionList != null) {
                if (fileExtensionList.contains(fileExtension) && ("xls".equalsIgnoreCase(fileExtension) || "xlx".equalsIgnoreCase(fileExtension) || "xlsx".equalsIgnoreCase(fileExtension))) {
                    fileDataBaseType = "CSV";
                } else if (fileExtensionList.contains(fileExtension) && ("xml".equalsIgnoreCase(fileExtension) || "xsd".equalsIgnoreCase(fileExtension))) {
                    fileDataBaseType = "XSD";
                } else if (fileExtensionList.contains(fileExtension) && "json".equalsIgnoreCase(fileExtension)) {
                    fileDataBaseType = "JSON";
                } else if (fileExtensionList.contains(fileExtension)) {
                    fileDataBaseType = "DSV";
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getTheFileDatabaseTypeBasedonTheFileExtension(-,-)");
        }
        return fileDataBaseType;

    }

    /**
     * this method will take the filePath as an input and return the
     * tableName(fileName) from that path
     *
     * @author : Dinesh Arasanakala
     * @param filePath
     * @return fileName
     */
    public static String returnTableNameFromFilePath(String filePath) {
        String fileExtension = "";
        String tableName = "";

        if (!StringUtils.isBlank(filePath)) {
            filePath = filePath.trim();

            String[] tableArray = null;

            if (!StringUtils.isBlank(filePath)) {

                filePath = FilenameUtils.normalizeNoEndSeparator(filePath, true);

                tableArray = filePath.split(";");
                for (String singleTable : tableArray) {

                    if (singleTable.contains("/")) {
                        filePath = singleTable;
                    }
                }

                try {
                    if (filePath.contains("/") && filePath.contains(".")) {
                        tableName = filePath.substring(filePath.lastIndexOf("/") + 1);
                        fileExtension = tableName.substring(tableName.lastIndexOf(".") + 1);
                        tableName = tableName.substring(0, tableName.lastIndexOf("."));
                    } else if (filePath.contains(".")) {
                        fileExtension = filePath.substring(filePath.lastIndexOf(".") + 1);
                        tableName = filePath.substring(0, filePath.lastIndexOf("."));
                    } else {
                        tableName = "";
                    }

                    if (StringUtils.isNotBlank(fileExtension)) {
                        fileExtension = fileExtension.replace("\"", "").trim();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    MappingManagerUtilAutomation.writeExeceptionLog(e, "returnTableNameFromFilePath(-,-)");
                }

            } else {
                tableName = "";
            }

        }
        if (!StringUtils.isBlank(fileExtension)) {
            tableName = tableName + Delimiter.fileExtensionDelimiter + fileExtension;
        }

        return tableName;
    }

    /**
     * this method returns query or query variable based on the access mode
     *
     * @param query
     * @param queryFromVarible
     * @param accessMode
     * @return
     */
    public static String gettingQueryBasedOnAccessMode(String query, String queryFromVarible, String accessMode) {

        if (accessMode.equalsIgnoreCase("2")) {
            queryFromVarible = "";
        } else if (accessMode.equalsIgnoreCase("3")) {
            query = "";
        }

        if (!StringUtils.isBlank(query)) {
            return query;
        } else if (!StringUtils.isBlank(queryFromVarible)) {
            return queryFromVarible;
        } else {
            return "";
        }

    }

    public static String getTableNameBasedOnAccesMode(String openRowsetTableName, String openRowsetVariableName, String accessMode, String componentDecision, String existingTableName) {
        String changedDelimiter = "@e_cr_u_os@";
        if (StringUtils.isNotBlank(openRowsetTableName) && "source".equalsIgnoreCase(componentDecision) && "0".equalsIgnoreCase(accessMode)) {
            existingTableName = openRowsetTableName + changedDelimiter;
        } else if (StringUtils.isNotBlank(openRowsetVariableName) && "source".equalsIgnoreCase(componentDecision) && "1".equalsIgnoreCase(accessMode)) {
            existingTableName = openRowsetVariableName + changedDelimiter;
        } else if (StringUtils.isNotBlank(openRowsetTableName) && "target".equalsIgnoreCase(componentDecision) && ("0".equalsIgnoreCase(accessMode) || "3".equalsIgnoreCase(accessMode))) {
            existingTableName = openRowsetTableName + changedDelimiter;
        } else if (StringUtils.isNotBlank(openRowsetVariableName) && "target".equalsIgnoreCase(componentDecision) && ("1".equalsIgnoreCase(accessMode) || "4".equalsIgnoreCase(accessMode))) {
            existingTableName = openRowsetVariableName + changedDelimiter;
        }

        return existingTableName;
    }

    /**
     * this method returns table name based on the access mode
     *
     * @param accessmode
     * @param componentDecision
     * @param tableName
     * @param componentClassId
     * @return
     */
    public static String makeTableNameAsEmptyBasedOnAccessMode(String accessmode, String componentDecision, String tableName, String componentClassId) {
        try {
//            if (accessmode.equalsIgnoreCase("1") && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.SSISODBCSrc")) {
//                tableName = "";
//            } else if (accessmode.equalsIgnoreCase("1") && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.SSISOracleSrc")) {
//                tableName = "";
//            } else if ((accessmode.equalsIgnoreCase("2") || accessmode.equalsIgnoreCase("3")) && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.OLEDBSource")) {
//                tableName = "";
//            } else if (accessmode.equalsIgnoreCase("2") && componentClassId.equalsIgnoreCase("Microsoft.ManagedComponentHost")) {
//                tableName = "";
//            } else if ((accessmode.equalsIgnoreCase("2") || accessmode.equalsIgnoreCase("3")) && componentClassId.equalsIgnoreCase("Microsoft.ExcelSource")) {
//                tableName = "";
//            } else if (accessmode.equalsIgnoreCase("2") && componentClassId.equalsIgnoreCase("Microsoft.ExcelDestination")) {
//                tableName = "";
//            } else if (accessmode.equalsIgnoreCase("2") && componentDecision.equalsIgnoreCase("target")) {
//                tableName = "";
//            }

            if ((accessmode.equalsIgnoreCase("1") && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.SSISODBCSrc"))
                    || (accessmode.equalsIgnoreCase("1") && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.SSISOracleSrc"))
                    || ((accessmode.equalsIgnoreCase("2") || accessmode.equalsIgnoreCase("3")) && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.OLEDBSource"))
                    || ((accessmode.equalsIgnoreCase("2") || accessmode.equalsIgnoreCase("3")) && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.ExcelSource"))
                    || (accessmode.equalsIgnoreCase("2") && componentClassId.equalsIgnoreCase("Microsoft.ManagedComponentHost"))
                    || (accessmode.equalsIgnoreCase("2") && componentClassId.equalsIgnoreCase("Microsoft.ExcelDestination"))
                    || (accessmode.equalsIgnoreCase("2") && componentDecision.equalsIgnoreCase("target"))) {
                tableName = "";
            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "makeTableNameAsEmptyBasedOnAccessMode(-,-)");
        }

        return tableName;
    }

    /**
     * this method returns empties query based on the access mode
     *
     * @param accessmode
     * @param componentDecision
     * @param query
     * @param componentClassId
     * @return
     */
    public static String makeQueryAsEmptyBasedOnAccessMode(String accessmode, String componentDecision, String query, String componentClassId) {

        try {

//            if (accessmode.equalsIgnoreCase("0") && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.SSISODBCSrc")) {
//                query = "";
//            } else if (accessmode.equalsIgnoreCase("0") && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.SSISOracleSrc")) {
//                query = "";
//            } else if ((accessmode.equalsIgnoreCase("0") || accessmode.equalsIgnoreCase("1")) && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.OLEDBSource")) {
//                query = "";
//            } else if (accessmode.equalsIgnoreCase("0") && componentClassId.equalsIgnoreCase("Microsoft.ManagedComponentHost")) {
//                query = "";
//            } else if ((accessmode.equalsIgnoreCase("0") || accessmode.equalsIgnoreCase("1")) && componentClassId.equalsIgnoreCase("Microsoft.ExcelSource")) {
//                query = "";
//            } else if ((accessmode.equalsIgnoreCase("0") || accessmode.equalsIgnoreCase("1")) && componentClassId.equalsIgnoreCase("Microsoft.ExcelDestination")) {
//                query = "";
//            } else if ((accessmode.equalsIgnoreCase("0") || accessmode.equalsIgnoreCase("1") || accessmode.equalsIgnoreCase("3") || accessmode.equalsIgnoreCase("4")) && componentDecision.equalsIgnoreCase("target")) {
//                query = "";
//            }
            if ((accessmode.equalsIgnoreCase("0") && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.SSISODBCSrc"))
                    || (accessmode.equalsIgnoreCase("0") && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.SSISOracleSrc"))
                    || ((accessmode.equalsIgnoreCase("0") || accessmode.equalsIgnoreCase("1")) && componentDecision.equalsIgnoreCase("source") && componentClassId.equalsIgnoreCase("Microsoft.OLEDBSource"))
                    || (accessmode.equalsIgnoreCase("0") && componentClassId.equalsIgnoreCase("Microsoft.ManagedComponentHost"))
                    || ((accessmode.equalsIgnoreCase("0") || accessmode.equalsIgnoreCase("1")) && componentClassId.equalsIgnoreCase("Microsoft.ExcelSource"))
                    || ((accessmode.equalsIgnoreCase("0") || accessmode.equalsIgnoreCase("1")) && componentClassId.equalsIgnoreCase("Microsoft.ExcelDestination"))
                    || ((accessmode.equalsIgnoreCase("0") || accessmode.equalsIgnoreCase("1") || accessmode.equalsIgnoreCase("3") || accessmode.equalsIgnoreCase("4")) && componentDecision.equalsIgnoreCase("target"))) {

                query = "";
            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "makeQueryAsEmptyBasedOnAccessMode(-,-)");
        }

        return query;
    }

    /**
     * in this method we are updating columns set with component name,DataFlow
     * name and componentClassId providing these 3 are manadatory
     *
     * @param list
     * @param componentName
     * @param componentClassId
     * @return updatedColumnSet
     */
    public static Set<Map<String, String>> updateDataFlowNameInColumnSet(Set<Map<String, String>> list, String componentName, String componentClassId) {
        Set<Map<String, String>> returnColumnSet = new HashSet();
        try {

            String dataFlowName = "datatflowname";
            if (list == null) {
                list = new HashSet<>();
            }

            for (Map<String, String> linkedHashMap : list) {
                try {
                    String inputColumnRefId = linkedHashMap.get("lineageid");
                    if (StringUtils.isNotBlank(inputColumnRefId) && inputColumnRefId.contains("\\")) {
                        String[] stringArr = inputColumnRefId.split("\\\\");
                        dataFlowName = stringArr[stringArr.length - 1];
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
                LinkedHashMap<String, String> newMap = new LinkedHashMap<>(linkedHashMap);
                newMap.put("dataflowname", dataFlowName);
                newMap.put("componentName", componentName);
                newMap.put("componentClassId", componentClassId);
                returnColumnSet.add(newMap);
            }
        } catch (Exception e) {

            MappingManagerUtilAutomation.writeExeceptionLog(e, "updateDataFlowNameInColumnSet(-,-)");
        }
        return returnColumnSet;
    }

    /**
     * this method will give tableName(file Name) and it's related information
     * from the component connection String
     *
     * @param connectionRefId
     * @param connectionsMap
     * @return table related information
     */
    public static String getTableRelatedInfoFromConnectionString(String connectionRefId, Map<String, String> connectionsMap, DataFlowBean dataFlowBean) {

        if (connectionRefId.contains(Constants.external) || connectionRefId.contains(Constants.invalid)) {
            connectionRefId = connectionRefId.replace(Constants.external, "").replace(Constants.invalid, "");
        }
        String objectName = "";
        if (connectionRefId.contains("[") && connectionRefId.contains("]")) {
            int startIndex = connectionRefId.indexOf("[");
            int endIndex = connectionRefId.indexOf("]");
            objectName = connectionRefId.substring(startIndex + 1, endIndex);
        }
        String connectionString = "";
        if (connectionsMap.get(connectionRefId) != null) {
            connectionString = connectionsMap.get(connectionRefId);
        } else if (connectionsMap.get(objectName) != null) {

            connectionString = connectionsMap.get(objectName);
        }
        String defaultConnectionString = "";
        try {

            String[] connectionStringArray = connectionString.split(Delimiter.connectionStringDelimiter);
            int length = connectionStringArray.length;

            switch (length) {
                case 2:
                    defaultConnectionString = connectionStringArray[1];
                    connectionString = connectionStringArray[0];
                    break;
                case 1:
                    connectionString = connectionStringArray[0];
                    break;
                default:
                    break;

            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getTableRelatedInfoFromConnectionString(-,-)");
        }
        String tableCombinationData = "";
        tableCombinationData = getTableNameAndDatabaseTypeFromTheConnectioString(connectionString, defaultConnectionString, dataFlowBean);

        return tableCombinationData;
    }

    /**
     * this method returns table,system,environment name for a given connection
     * string
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
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getTableAndSystemAndEnvNameFromConnectionString(-,-)");
        }
        return "";
    }

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
     * in this method we are making empty some of the values for the dataflow
     * component iteration
     *
     * @param dataFlowBean
     */
    public static void makeDataFlowSetterMethodsAsEmpty(DataFlowBean dataFlowBean) {
        dataFlowBean.setSheetName("");
        dataFlowBean.setAccessMode("");
        dataFlowBean.setCollectionNameForODataSource("");
        dataFlowBean.setFileExtension("");
        dataFlowBean.setIsFileComponent(false);
        dataFlowBean.setFileDataBaseType("");
        dataFlowBean.setXmlData("");
        dataFlowBean.setRawSourceVariableFileName("");
        dataFlowBean.setRawSourceFileName("");
        dataFlowBean.setXmlDataVariable("");
    }

    public static String replaceSpecialCharaterFromEnd(String input) {
        String str = "";
        try {
            char[] charArry = input.toCharArray();
            int length = charArry.length;

            for (int i = length - 1; i >= 0; i--) {
                char c = charArry[i];
                if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
                    str = input.substring(0, input.lastIndexOf(c) + 1);
                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return input;
        }
        return str;
    }
}
