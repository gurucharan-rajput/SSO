/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util.variablereplacement;

import com.erwin.cfx.connectors.ssis.generic.controller.SSISController;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.MappingManagerUtilAutomation;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.NodeList;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 18-08-2021
 */
@SuppressWarnings("all")
public class DynamicVaribleValueReplacement {

    DynamicVaribleValueReplacement() {
    }

    public static Map<String, String> globalUserReferenceMap = new HashMap<>();
    public static Map<String, String> globalProjectReferenceHashMap = new HashMap<>();
    public static Map<String, String> globalChildLevelHashMap = new HashMap<>();
    public static Map<String, String> globalPackageReferenceHashMap = new HashMap<>();

    public static void clearStaticVaiables() {
        globalUserReferenceMap = new HashMap<>();
        globalProjectReferenceHashMap = new HashMap<>();
        globalChildLevelHashMap = new HashMap<>();
        globalPackageReferenceHashMap = new HashMap<>();

    }

    public static void getChildPackageNameAndVaribleData(NodeList executableList, int l, String objectName, Map<String, HashMap<String, HashMap<String, String>>> parentComponentAndChildPackageName, Map<String, String> parameterHashMap) {
        try {
            NodeList ObjectDataExecutableList = executableList.item(l).getChildNodes();

            for (int obj = 0; obj < ObjectDataExecutableList.getLength(); obj++) {

                if ("DTS:ObjectData".equalsIgnoreCase(ObjectDataExecutableList.item(obj).getNodeName())) {
                    NodeList ExecutePackageTaskList = ObjectDataExecutableList.item(obj).getChildNodes();
                    for (int exe = 0; exe < ExecutePackageTaskList.getLength(); exe++) {
                        if ("ExecutePackageTask".equalsIgnoreCase(ExecutePackageTaskList.item(exe).getNodeName())) {
                            NodeList childPackageTaskList = ExecutePackageTaskList.item(exe).getChildNodes();
                            String packageName = "";
                            for (int childPackage = 0; childPackage < childPackageTaskList.getLength(); childPackage++) {
                                HashMap<String, HashMap<String, String>> childPackageAndVaribleMap = new HashMap<>();

                                if ("PackageName".equalsIgnoreCase(childPackageTaskList.item(childPackage).getNodeName())) {

                                    packageName = childPackageTaskList.item(childPackage).getTextContent();

                                    HashMap<String, String> varibleMap = new HashMap<>();
                                    NodeList varaiblesNodeList = childPackageTaskList.item(childPackage).getParentNode().getParentNode().getParentNode().getParentNode().getParentNode().getChildNodes();

                                    varibleMap = new HashMap<>();
                                    String varibleName = "";
                                    String varibleValueName = "";
                                    for (int varibleList = 0; varibleList < varaiblesNodeList.getLength(); varibleList++) {
                                        if ("DTS:Variables".equalsIgnoreCase(varaiblesNodeList.item(varibleList).getNodeName())) {

                                            NodeList variableChildNodeList = varaiblesNodeList.item(varibleList).getChildNodes();
                                            for (int i = 0; i < variableChildNodeList.getLength(); i++) {

                                                if ("DTS:Variable".equalsIgnoreCase(variableChildNodeList.item(i).getNodeName())) {
                                                    varibleName = variableChildNodeList.item(i).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                                                    NodeList VariableValueNodeList = variableChildNodeList.item(i).getChildNodes();
                                                    for (int VariableValueListInt = 0; VariableValueListInt < VariableValueNodeList.getLength(); VariableValueListInt++) {
                                                        if ("DTS:VariableValue".equalsIgnoreCase(VariableValueNodeList.item(VariableValueListInt).getNodeName())) {
                                                            varibleValueName = VariableValueNodeList.item(VariableValueListInt).getTextContent();
                                                        }

                                                    }

                                                    if (!StringUtils.isBlank(varibleName)) {
                                                        varibleMap.put(varibleName, varibleValueName);
                                                    }

                                                }

                                            }

                                        }

                                    }
                                    if (!StringUtils.isBlank(packageName)) {
                                        childPackageAndVaribleMap.put(packageName, varibleMap);
                                        parentComponentAndChildPackageName.put(objectName, childPackageAndVaribleMap);
                                    }

                                }

                                //parameter binding
                                String parameterName = "";
                                String bindedVariableOrParameterName = "";
                                if ("ParameterAssignment".equalsIgnoreCase(childPackageTaskList.item(childPackage).getNodeName())) {
                                    NodeList ParameterAssignmentChildList = childPackageTaskList.item(childPackage).getChildNodes();
                                    for (int pa = 0; pa < ParameterAssignmentChildList.getLength(); pa++) {

                                        if ("ParameterName".equalsIgnoreCase(ParameterAssignmentChildList.item(pa).getNodeName())) {
                                            parameterName = ParameterAssignmentChildList.item(pa).getTextContent();
                                        }
                                        if ("BindedVariableOrParameterName".equalsIgnoreCase(ParameterAssignmentChildList.item(pa).getNodeName())) {
                                            bindedVariableOrParameterName = ParameterAssignmentChildList.item(pa).getTextContent();
                                        }

                                        if (StringUtils.isNotBlank(objectName) && StringUtils.isNotBlank(packageName) && StringUtils.isNotBlank(parameterName) && StringUtils.isBlank(bindedVariableOrParameterName)) {

                                            String key = objectName + "@ERWIN@" + packageName + "@ERWIN@" + bindedVariableOrParameterName;
                                            parameterHashMap.put(key, parameterName);

                                        }

                                    }
                                }

                            }

                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getChildPackageNameAndVaribleData(-,-)");
        }

    }

    public static HashMap<String, String> replaceVaribleNameWithActualQueryInTheExtendedPropertesMap(Map<String, String> packageLevelVariablesMap, HashMap<String, String> extendedPropertiesMap, HashSet<String> controlFlowComponentNameSet, Map<String, String> projectLevelVariablesMap) {

        HashMap<String, String> replacedExtendedPropsMap = new HashMap<>();
        String projectContsant = "$Project";
        String querySeparator = "separator";

        for (Map.Entry<String, String> extendedPropsEntrySet : extendedPropertiesMap.entrySet()) {

            String componentName = extendedPropsEntrySet.getKey();
            String query = extendedPropsEntrySet.getValue();
            try {
                if (query.contains("::")) {

                    String tempQuery = query;
                    tempQuery = query.split(Delimiter.delimiter)[0];
                    String packageOrProjectReference = tempQuery.split("::")[0];
                    String actualUserOrPackageName = tempQuery.split("::")[1];

                    if (!StringUtils.isBlank(packageOrProjectReference)
                            && "user".equalsIgnoreCase(packageOrProjectReference.trim())
                            && packageLevelVariablesMap.get(actualUserOrPackageName) != null) {
                        String correspondingPackageVaribleValue = packageLevelVariablesMap.get(actualUserOrPackageName);
                        query = query.replace(packageOrProjectReference + "::" + actualUserOrPackageName, correspondingPackageVaribleValue);
                    } else if (!StringUtils.isBlank(packageOrProjectReference)
                            && projectContsant.equalsIgnoreCase(packageOrProjectReference.trim())
                            && projectLevelVariablesMap.get(actualUserOrPackageName) != null) {
                        String correspondingProjectVaribleValue = projectLevelVariablesMap.get(actualUserOrPackageName);
                        query = query.replace(packageOrProjectReference + "::" + actualUserOrPackageName, correspondingProjectVaribleValue);
                    } else {
                        query = query.replace(packageOrProjectReference + "::" + actualUserOrPackageName, actualUserOrPackageName);
                    }
                    replacedExtendedPropsMap.put(componentName, query + querySeparator + actualUserOrPackageName);
                } else {
                    replacedExtendedPropsMap.put(componentName, query);
                }
            } catch (Exception e) {
                e.printStackTrace();
                MappingManagerUtilAutomation.writeExeceptionLog(e, "replaceVaribleNameWithActualQueryInTheExtendedPropertesMap(-,-)");
            }

        }

        replacedExtendedPropsMap = replaceTheQueyNameFromTheHashmapKeyWithvaribleName(replacedExtendedPropsMap, controlFlowComponentNameSet);
        return replacedExtendedPropsMap;
    }

    public static HashMap<String, String> replaceTheQueyNameFromTheHashmapKeyWithvaribleName(Map<String, String> replacedExtendedPropsMap, Set<String> controlFlowComponentNameSet) {

        HashMap<String, String> replacedExtendedPropsMapwithVarbleName = new HashMap<>();

        for (Map.Entry<String, String> extMapEntrySet : replacedExtendedPropsMap.entrySet()) {
            String componentName = extMapEntrySet.getKey();
            String varibleExpression = extMapEntrySet.getValue();
            if (varibleExpression.contains("separator")) {

                String varibleName = varibleExpression.split("separator")[1];
                String varibleExp = varibleExpression.split("separator")[0];
                controlFlowComponentNameSet.add(componentName);
                componentName = componentName.replace("queryName", varibleName);

                replacedExtendedPropsMapwithVarbleName.put(componentName, varibleExp);

            } else {
                controlFlowComponentNameSet.add(componentName);
                componentName = componentName.replace("queryName", "");

                replacedExtendedPropsMapwithVarbleName.put(componentName, varibleExpression);
            }

        }
        return replacedExtendedPropsMapwithVarbleName;
    }

    public static List<Map<String, String>> replaceSpecialSymobalsFromPackageAndProjectNameAndMakeOnlyThierNamesAsValues(Map<String, String> userReferenceMap, Map<String, String> projectReferenceMap, Map<String, String> childLevelVaribleMap, Map<String, String> packageRefrenceVariableMap) {
        Map<String, String> packageLevelReplacedMap = new HashMap<>();
        Map<String, String> projectLevelReplacedMap = new HashMap<>();
        List<Map<String, String>> updatedPackageAndProjectList = new ArrayList<>();
        clearStaticVaiables();
        if (userReferenceMap != null) {
            globalUserReferenceMap = userReferenceMap;
        }

        if (projectReferenceMap != null) {
            globalProjectReferenceHashMap = projectReferenceMap;
        }

        if (childLevelVaribleMap != null) {
            globalChildLevelHashMap = childLevelVaribleMap;
        }
        if (packageRefrenceVariableMap != null) {
            globalPackageReferenceHashMap = packageRefrenceVariableMap;
        }

        try {
            for (Map.Entry<String, String> packagLevelEntrySet : globalUserReferenceMap.entrySet()) {
                String packageLevelVariableName = packagLevelEntrySet.getKey();
                String packageLevelVariableValue = packagLevelEntrySet.getValue();
                packageLevelVariableValue = getActualNameWithOutSpecialSymbols(packageLevelVariableValue, "user", packageLevelVariableName);
                String variableReplacementFlag = "";
                if (!StringUtils.isBlank(packageLevelVariableValue) && packageLevelVariableValue.split(Delimiter.variableReplacementDelimiter).length >= 2) {
                    variableReplacementFlag = packageLevelVariableValue.split(Delimiter.variableReplacementDelimiter)[1];
                    packageLevelVariableValue = packageLevelVariableValue.split(Delimiter.variableReplacementDelimiter)[0];
                }
                if ("true".equalsIgnoreCase(variableReplacementFlag)) {
                    packageLevelVariableValue = removeSpaceAfterDotAndPlusSymbolsFromVariableValues(packageLevelVariableValue);
                }

                packageLevelReplacedMap.put(packageLevelVariableName, packageLevelVariableValue);
            }
            for (Map.Entry<String, String> projectLevelEntrySet : globalProjectReferenceHashMap.entrySet()) {
                String projectLevelVariableName = projectLevelEntrySet.getKey();
                String projectLevelVariableValue = projectLevelEntrySet.getValue();
                projectLevelVariableValue = getActualNameWithOutSpecialSymbols(projectLevelVariableValue, "$Project", projectLevelVariableName);
                String variableReplacementFlag = "";
                if (!StringUtils.isBlank(projectLevelVariableValue) && projectLevelVariableValue.split(Delimiter.variableReplacementDelimiter).length >= 2) {
                    variableReplacementFlag = projectLevelVariableValue.split(Delimiter.variableReplacementDelimiter)[1];
                    projectLevelVariableValue = projectLevelVariableValue.split(Delimiter.variableReplacementDelimiter)[0];
                }
                if ("true".equalsIgnoreCase(variableReplacementFlag)) {
                    projectLevelVariableValue = removeSpaceAfterDotAndPlusSymbolsFromVariableValues(projectLevelVariableValue);
                }

                projectLevelReplacedMap.put(projectLevelVariableName, projectLevelVariableValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "replaceSpecialSymobalsFromPackageAndProjectNameAndMakeOnlyThierNamesAsValues(-,-)");
        }
        updatedPackageAndProjectList.add(packageLevelReplacedMap);
        updatedPackageAndProjectList.add(projectLevelReplacedMap);

        return updatedPackageAndProjectList;

    }

    /**
     * In this method,the User reference present in the user level variable hash
     * map is replaced with the actual value
     *
     * @param packageLevelMap
     * @return
     */
    public static Map<String, String> replaceSpecialSymobalsFromPackageAndProjectNameAndMakeOnlyThierNamesAsValues_2008(Map<String, String> packageLevelMap) {
        Map<String, String> packageLevelReplacedMap = new HashMap();

        clearStaticVaiables();
        if (packageLevelMap != null) {
            globalUserReferenceMap = packageLevelMap;
        }

        try {
            globalUserReferenceMap.entrySet().forEach((Map.Entry<String, String> packagLevelEntrySet) -> {
                String packageLevelVariableName = packagLevelEntrySet.getKey();
                String packageLevelVariableValue = packagLevelEntrySet.getValue();
                packageLevelVariableValue = getActualNameWithOutSpecialSymbols(packageLevelVariableValue, "user", packageLevelVariableName);
                String variableReplacementFlag = "";
                if (!StringUtils.isBlank(packageLevelVariableValue) && packageLevelVariableValue.split(Delimiter.variableReplacementDelimiter).length >= 2) {
                    variableReplacementFlag = packageLevelVariableValue.split(Delimiter.variableReplacementDelimiter)[1];
                    packageLevelVariableValue = packageLevelVariableValue.split(Delimiter.variableReplacementDelimiter)[0];
                }
                if ("true".equalsIgnoreCase(variableReplacementFlag)) {
                    packageLevelVariableValue = removeSpaceAfterDotAndPlusSymbolsFromVariableValues(packageLevelVariableValue);
                }

                packageLevelReplacedMap.put(packageLevelVariableName, packageLevelVariableValue);
            });

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "replaceSpecialSymobalsFromPackageAndProjectNameAndMakeOnlyThierNamesAsValues_2008(-,-)");
        }

        return packageLevelReplacedMap;

    }

    /**
     * In this method,the user or package or project reference variables are
     * replaced with the actual value
     *
     * @param varibleValue
     * @param packageOrProjectType
     * @param packageOrProjectVariableName
     * @return
     */
    @SuppressWarnings("all")
    public static String getActualNameWithOutSpecialSymbols(String varibleValue, String packageOrProjectType, String packageOrProjectVariableName) {
        String test[] = null;
        String variableReplacementFlag = "false";
        int count = 0;
        try {
            while (varibleValue.contains("@[") && varibleValue.contains("::") && varibleValue.contains("]") && count < 100) {

                test = varibleValue.split("\\@\\[");

                for (String test2 : test) {

                    if (test2.contains("::")) {
                        String packageOrProjectReference = test2.split("::")[0];
                        String actualUserOrPackageName = test2.split("::")[1].split("]")[0];

                        String variableCallingFormation = packageOrProjectReference + "::" + actualUserOrPackageName;

                        if ("user".equalsIgnoreCase(packageOrProjectReference)
                                && packageOrProjectType.equalsIgnoreCase(packageOrProjectReference)
                                && actualUserOrPackageName.equalsIgnoreCase(packageOrProjectVariableName)) {
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", actualUserOrPackageName);
                        } else if ("user".equalsIgnoreCase(packageOrProjectReference)
                                && packageOrProjectType.equalsIgnoreCase(packageOrProjectReference)
                                && globalUserReferenceMap.get(actualUserOrPackageName) != null
                                && globalUserReferenceMap.get(actualUserOrPackageName).contains(variableCallingFormation)) {
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", actualUserOrPackageName);
                        } else if ("$Project".equalsIgnoreCase(packageOrProjectReference)
                                && packageOrProjectType.equalsIgnoreCase(packageOrProjectReference)
                                && actualUserOrPackageName.equalsIgnoreCase(packageOrProjectVariableName)) {
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", actualUserOrPackageName);
                        } else if ("$Project".equalsIgnoreCase(packageOrProjectReference)
                                && packageOrProjectType.equalsIgnoreCase(packageOrProjectReference)
                                && globalProjectReferenceHashMap.get(actualUserOrPackageName) != null
                                && globalProjectReferenceHashMap.get(actualUserOrPackageName).contains(variableCallingFormation)) {
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", actualUserOrPackageName);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "user".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalUserReferenceMap.get(actualUserOrPackageName) != null) {
                            variableReplacementFlag = "true";
                            String correspondingPackageVaribleValue = globalUserReferenceMap.get(actualUserOrPackageName);
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingPackageVaribleValue);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "$Project".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalProjectReferenceHashMap.get(actualUserOrPackageName) != null) {
                            variableReplacementFlag = "true";
                            String correspondingProjectVaribleValue = globalProjectReferenceHashMap.get(actualUserOrPackageName);
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingProjectVaribleValue);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "$Package".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalPackageReferenceHashMap.get(actualUserOrPackageName) != null) {
                            variableReplacementFlag = "true";
                            String correspondingProjectVaribleValue = globalPackageReferenceHashMap.get(actualUserOrPackageName);
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingProjectVaribleValue);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "$Package".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalChildLevelHashMap.get(actualUserOrPackageName) != null) {
                            variableReplacementFlag = "true";
                            String correspondingProjectVaribleValue = globalChildLevelHashMap.get(actualUserOrPackageName);
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingProjectVaribleValue);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "System".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalUserReferenceMap.get(actualUserOrPackageName) != null) {
                            variableReplacementFlag = "true";
                            String correspondingProjectVaribleValue = globalUserReferenceMap.get(actualUserOrPackageName);
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingProjectVaribleValue);
                        } else {
//                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", "[" + actualUserOrPackageName + "]");
                            varibleValue = varibleValue.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", actualUserOrPackageName);
                        }

                    }
                }
                count++;

            }
            if (count == 100) {
                SSISController.logger.info("@@@ the file is falling into an infinite loop @@@ ");
//                CallSSIS2015ReverseEngineering.writeLogsToFile(logFile, fileDate, " @@@ the file is falling into an infinite loop @@@ ");
            }
        } catch (Exception e) {
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getActualNameWithOutSpecialSymbols(-,-)");
            e.printStackTrace();
        }
        return varibleValue + Delimiter.variableReplacementDelimiter + variableReplacementFlag;
    }

    public static String getActualNameWithOutSpecialSymbols(String coneectionString, String dfVariableName, String dfVariableValue, String dfPkgOrProjectReference, String fileOrDBComponent) {
        String test[] = null;

        String getDateReplacementFlag = "false";
        int count = 0;
        try {
            while (coneectionString.contains("@[") && coneectionString.contains("::") && coneectionString.contains("]") && count < 100) {

                test = coneectionString.split("\\@\\[");

                for (String test2 : test) {

                    if (test2.contains("::")) {
                        String packageOrProjectReference = test2.split("::")[0];
                        String actualUserOrPackageName = test2.split("::")[1].split("]")[0];

                        String variableCallingFormation = packageOrProjectReference + "::" + actualUserOrPackageName;

                        if ("user".equalsIgnoreCase(dfPkgOrProjectReference)
                                //                                && globalPackageLevelMap.get(actualUserOrPackageName) == null
                                && actualUserOrPackageName.equalsIgnoreCase(dfVariableName)
                                && !StringUtils.isBlank(dfVariableName)
                                && !StringUtils.isBlank(dfVariableValue)) {
//                            variableReplacementFlag = "true";
                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", dfVariableValue);
                        } else if ("user".equalsIgnoreCase(packageOrProjectReference)
                                && packageOrProjectReference.equalsIgnoreCase(dfPkgOrProjectReference)
                                && globalUserReferenceMap.get(actualUserOrPackageName) != null
                                && globalUserReferenceMap.get(actualUserOrPackageName).contains(variableCallingFormation)) {
                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", actualUserOrPackageName);
                        } else if ("$Project".equalsIgnoreCase(dfPkgOrProjectReference)
                                //                                && globalProjectLevelHashMap.get(actualUserOrPackageName) == null
                                && actualUserOrPackageName.equalsIgnoreCase(dfVariableName)
                                && !StringUtils.isBlank(dfVariableName)
                                && !StringUtils.isBlank(dfVariableValue)) {
//                            variableReplacementFlag = "true";
                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", dfVariableValue);
                        } else if ("$Project".equalsIgnoreCase(packageOrProjectReference)
                                && packageOrProjectReference.equalsIgnoreCase(dfPkgOrProjectReference)
                                && globalProjectReferenceHashMap.get(actualUserOrPackageName) != null
                                && globalProjectReferenceHashMap.get(actualUserOrPackageName).contains(variableCallingFormation)) {
                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", actualUserOrPackageName);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "user".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalUserReferenceMap.get(actualUserOrPackageName) != null) {
//                            variableReplacementFlag = "true";
                            String correspondingPackageVaribleValue = globalUserReferenceMap.get(actualUserOrPackageName);

                            if (!StringUtils.isBlank(correspondingPackageVaribleValue)) {
                                if ("fileTypeComponent".equalsIgnoreCase(fileOrDBComponent) && correspondingPackageVaribleValue.contains("GETDATE(")) {
                                    String currentSystemDate = returnCurrentSystemDate();
                                    correspondingPackageVaribleValue = currentSystemDate;
                                    getDateReplacementFlag = "true";
                                }
                            }
                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingPackageVaribleValue);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "$Project".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalProjectReferenceHashMap.get(actualUserOrPackageName) != null) {
//                            variableReplacementFlag = "true";
                            String correspondingProjectVaribleValue = globalProjectReferenceHashMap.get(actualUserOrPackageName);
                            if (!StringUtils.isBlank(correspondingProjectVaribleValue)) {
                                if ("fileTypeComponent".equalsIgnoreCase(fileOrDBComponent) && correspondingProjectVaribleValue.contains("GETDATE(")) {
                                    String currentSystemDate = returnCurrentSystemDate();
                                    correspondingProjectVaribleValue = currentSystemDate;
                                    getDateReplacementFlag = "true";
                                }
                            }

                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingProjectVaribleValue);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "$Package".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalPackageReferenceHashMap.get(actualUserOrPackageName) != null) {
//                            variableReplacementFlag = "true";
                            String correspondingPackageVaribleValue = globalPackageReferenceHashMap.get(actualUserOrPackageName);
                            if ("fileTypeComponent".equalsIgnoreCase(fileOrDBComponent) && correspondingPackageVaribleValue.contains("GETDATE(")) {
                                String currentSystemDate = returnCurrentSystemDate();
                                correspondingPackageVaribleValue = currentSystemDate;
                                getDateReplacementFlag = "true";
                            }
                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingPackageVaribleValue);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "$Package".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalChildLevelHashMap.get(actualUserOrPackageName) != null) {
//                            variableReplacementFlag = "true";
                            String correspondingPackageVaribleValue = globalChildLevelHashMap.get(actualUserOrPackageName);
                            if ("fileTypeComponent".equalsIgnoreCase(fileOrDBComponent) && correspondingPackageVaribleValue.contains("GETDATE(")) {
                                String currentSystemDate = returnCurrentSystemDate();
                                correspondingPackageVaribleValue = currentSystemDate;
                                getDateReplacementFlag = "true";
                            }
                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingPackageVaribleValue);
                        } else if (!StringUtils.isBlank(packageOrProjectReference)
                                && "System".equalsIgnoreCase(packageOrProjectReference.trim())
                                && globalUserReferenceMap.get(actualUserOrPackageName) != null) {
//                            variableReplacementFlag = "true";
                            String correspondingProjectVaribleValue = globalUserReferenceMap.get(actualUserOrPackageName);
                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", correspondingProjectVaribleValue);
                        } else {
//                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", "[" + actualUserOrPackageName + "]");
                            coneectionString = coneectionString.replace("@[" + packageOrProjectReference + "::" + actualUserOrPackageName + "]", actualUserOrPackageName);
                        }

                    }
                }
                count++;
            }
            if (count == 100) {
                SSISController.logger.info("@@@ the file is falling into an infinite loop @@@ ");
            }
        } catch (Exception e) {
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getActualNameWithOutSpecialSymbols(-,-)");
            e.printStackTrace();
        }
        return coneectionString + Delimiter.getDateReplacementDelimiter + getDateReplacementFlag;
//        return coneectionString;
    }

    public static String returnCurrentSystemDate() {
        String currentSystemDate = "";
        try {
            String currentDataFormatString = "yyyyMMdd";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(currentDataFormatString);
            currentSystemDate = simpleDateFormat.format(new Date());
        } catch (Exception e) {
            MappingManagerUtilAutomation.writeExeceptionLog(e, "returnCurrentSystemDate(-,-)");
            e.printStackTrace();
        }
        return currentSystemDate;
    }

    public static String getActualNameWithOutSpecialSymbolsForDerivedComponentBusinessRule(String varibleValue, Map<String, String> varibleOrProjectLevelMap) {
        String test[] = null;
        try {
            if (varibleValue.contains("@[")) {

                test = varibleValue.split("\\@\\[");

                for (String test2 : test) {

                    if (test2.contains("::")) {
                        String systemOrUserName = test2.split("::")[0];
                        String actualUserOrPackageName = test2.split("::")[1].split("]")[0];
                        if (varibleOrProjectLevelMap.get(actualUserOrPackageName) != null) {

                            String actualValue = varibleOrProjectLevelMap.get(actualUserOrPackageName);
                            varibleValue = varibleValue.replace("@[" + systemOrUserName + "::" + actualUserOrPackageName + "]", "(" + actualValue + ")");
                        }

                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getActualNameWithOutSpecialSymbolsForDerivedComponentBusinessRule(-,-)");
        }
        return varibleValue;
    }

    public static String removeSpaceAfterDotAndPlusSymbolsFromVariableValues(String varibleValue) {
        varibleValue = varibleValue.replaceAll("\\.\\s+", ".");
        try {
            String test = "";
//                             String test = "select top 10 * from+ OPENQUERY (\n"
//                + "   [JIRACONN], 'SELECT * FROM jiraissue where PROJECT = \"+ @[User::idProyecto] +   \" ')";
            test = varibleValue.replaceAll("\"\\s*\\+", "\"").replaceAll("\\+\\s*\"", "\"");
            if (!StringUtils.isBlank(test) && !(test.toUpperCase().contains("EXECUTE ") || test.toUpperCase().contains("EXEC "))) {
                test = test.replaceAll("\"\\s*", "\"").replaceAll("\\s*\"", " ");
            } else {
                test = test.replace("\"", "");
            }

            varibleValue = test;

        } catch (Exception e) {
             MappingManagerUtilAutomation.writeExeceptionLog(e, "removeSpaceAfterDotAndPlusSymbolsFromVariableValues(-,-)");
            e.printStackTrace();
        }
        return varibleValue;
    }

    public static HashMap<String, String> replaceBindedParameterValuesWithOrginalValuesFromParentPackageIntoChildPackage(Map<String, String> varibleMap, HashMap<String, String> childLevelVaribleMap) {
        HashMap<String, String> replacedValueMap = new HashMap<>();
        boolean flag = false;

        for (Map.Entry<String, String> childLevelVaribleMapEntrySet : childLevelVaribleMap.entrySet()) {
            String key = childLevelVaribleMapEntrySet.getKey();
            String value = childLevelVaribleMapEntrySet.getValue();

            for (Map.Entry<String, String> varibleMapEntrySet : varibleMap.entrySet()) {
                String varibleName = varibleMapEntrySet.getKey();
                String expressionValue = varibleMapEntrySet.getValue();
                flag = false;
                if (expressionValue.contains(key)) {
                    expressionValue = expressionValue.replace("[" + key + "]", value).replaceAll("\\.\\s+", ".");
//                    expressionValue = expressionValue.replaceAll("\\.\\s+", ".");
                    try {
                        String test = "";

                        test = expressionValue.replaceAll("\"\\s*\\+", "\"").replaceAll("\\+\\s*\"", "\"");
                        if (!StringUtils.isBlank(test) && !(test.toUpperCase().contains("EXECUTE ") || test.toUpperCase().contains("EXEC "))) {
                            test = test.replaceAll("\"\\s*", "\"").replaceAll("\\s*\"", "");
                        } else {
                            test = test.replace("\"", "");
                        }
                        expressionValue = test;
                    } catch (Exception e) {
                        MappingManagerUtilAutomation.writeExeceptionLog(e, "replaceBindedParameterValuesWithOrginalValuesFromParentPackageIntoChildPackage(-,-)");
                        e.printStackTrace();
                    }
                    flag = true;
                }

                if (replacedValueMap.get(varibleName) == null || flag) {

//                    expressionValue = expressionValue.replace("@[$Package::", "").replace("]", "").replace("+", "").replace("\"", "");
                    replacedValueMap.put(varibleName, expressionValue);
                }

            }

        }

        return replacedValueMap;
    }

    public static HashMap<String, String> prepareNewchildLevelVaribleMapWithVariableAndParameterHashMap(HashMap<String, String> childLevelMap, Map<String, String> parameterHashMap, String componentName) {

        HashMap<String, String> childLevelVaribleMap = new HashMap<>();

        try {
            for (Map.Entry<String, String> childLevelEntrySet : childLevelMap.entrySet()) {

                String key = childLevelEntrySet.getKey();
                String value = childLevelEntrySet.getValue();
                for (Map.Entry<String, String> parameterHashMapEntrySet : parameterHashMap.entrySet()) {

                    String key1 = parameterHashMapEntrySet.getKey();
                    String value1 = parameterHashMapEntrySet.getValue();
                    if (key1.contains(key) && key1.contains(componentName)) {
                        childLevelVaribleMap.put(value1, value);
                        break;
                    }

                }

            }
        } catch (Exception e) {
             MappingManagerUtilAutomation.writeExeceptionLog(e, "prepareNewchildLevelVaribleMapWithVariableAndParameterHashMap(-,-)");
            e.printStackTrace();
        }

        return childLevelVaribleMap;
    }

    public static String returnVariableOrProjectNameWithSplitingUserOrProjectReference(String variableOrProjetName) {
        String actualVariableName = "";
        String actualVariableReference = "";
        try {
            String[] variableOrProjetNameArray = variableOrProjetName.split("::");
            int splitLength = variableOrProjetNameArray.length;

            switch (splitLength) {
                case 2:
                    actualVariableReference = variableOrProjetNameArray[0];
                    actualVariableName = variableOrProjetNameArray[1];
                    break;
                case 1:
                    actualVariableReference = variableOrProjetNameArray[0];
                    break;
                default:
                    break;
            }

        } catch (Exception e) {
               MappingManagerUtilAutomation.writeExeceptionLog(e, "returnVariableOrProjectNameWithSplitingUserOrProjectReference(-,-)");
            e.printStackTrace();
        }

        return actualVariableReference + Delimiter.delimiter + actualVariableName;
    }
}
