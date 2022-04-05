/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

import com.ads.api.beans.common.AuditHistory;
import com.ads.api.beans.common.Node;
import com.ads.api.beans.kv.KeyValue;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.mm.Project;
import com.ads.api.beans.mm.Subject;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.controller.SSISController;
import com.icc.util.ApplicationConstants;
import com.icc.util.RequestStatus;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author sadar/Dinesh Arasanakala/Harika
 * @Date:16-08-2021
 */
public class MappingManagerUtilAutomation {

    public static String globalSubjectHierarchy = "";

    /**
     *
     * This method will return the project id if the project is already their it
     * will return projectId directly other wise it will create the project and
     * return the projectId.
     *
     * @param projectName
     * @param mappingManagerUtil
     *
     * @return the projectId .
     *
     */
    public int returnTheProjectId(String projectName, MappingManagerUtil mappingManagerUtil) {
        int projectId = 0;
        try {
            if (mappingManagerUtil.getProject(projectName) != null) {
                projectId = mappingManagerUtil.getProject(projectName).getProjectId();
            } else {
                Project project = new Project();
                project.setProjectName(projectName);
                mappingManagerUtil.createProject(project).getStatusMessage();
                projectId = mappingManagerUtil.getProject(projectName).getProjectId();
            }
        } catch (Exception e) {
            SSISController.logger.info("methodName :--returnTheProjectId(-,-)");
            if (e.getMessage().equalsIgnoreCase("getAttribute: Session already invalidated")) {
                SSISController.logger.info("Session already invalidated");
            }

            SSISController.logger.error(e.getMessage());
            e.printStackTrace();
            projectId = 0;
        }

        return projectId;

    }

    /**
     *
     * this method will create subject providing subjectName and parentSubjectId
     * or projectName is mandatory.
     *
     * @param projectId
     * @param mappingManagerUtil
     * @param subjectName
     * @param projectName
     * @param logFlag
     * @param fileDate
     * @param logFile
     * @return the subject creation status
     */
    public String createSubject(int projectId, MappingManagerUtil mappingManagerUtil, String subjectName, String projectName) {

        StringBuilder sb = new StringBuilder();
        try {
            int subId = -1;
            subjectName = subjectName.replaceAll(Constants.commonRegularExpression, "_");
            if (subjectName.endsWith("_")) {
                subjectName = subjectName.substring(0, subjectName.lastIndexOf("_"));
            }
            try {
                subId = getSubjectId(subjectName, 0, projectName, mappingManagerUtil);
                if (subId > 0) {
                    sb.append("subject creation msg :" + "Subject Is Already Avaliable. Subject Name : " + subjectName + "\n\n");
                } else {

                    Subject subjectDetails = new Subject();

                    subjectDetails.setSubjectName(subjectName);
                    subjectDetails.setSubjectDescription("Description");
                    AuditHistory auditHistory = new AuditHistory();
                    auditHistory.setCreatedBy("Administrator");
                    subjectDetails.setAuditHistory(auditHistory);
                    subjectDetails.setProjectId(projectId);
                    subjectDetails.setConsiderUserDefinedFlag("Y");
                    subjectDetails.setParentSubjectId(-1);
                    RequestStatus retRS = mappingManagerUtil.createSubject(subjectDetails);
                    sb.append("subject creation msg " + retRS.getStatusMessage() + "\n\n");

                }
            } catch (Exception e) {

                SSISController.logger.info("methodName :--createSubject(-,-)");
                if (e.getMessage().equalsIgnoreCase("getAttribute: Session already invalidated")) {
                    SSISController.logger.info("Session already invalidated");
                }

                SSISController.logger.error(e.getMessage());
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    /**
     * this method will create child subject providing subjectName and
     * parentSubjectId is mandatory.
     *
     * @param subjectName
     * @param projectId
     * @param subId
     * @param mappingManagerUtil
     * @param projectName
     * @return the subject creation status
     */
    public String createChildSubject(String subjectName, int projectId, int subId, MappingManagerUtil mappingManagerUtil, String projectName, String subjectDescription) {

        StringBuilder sb = new StringBuilder();
        subjectName = subjectName.replaceAll(Constants.commonRegularExpression, "_");
        if (subjectName.endsWith("_")) {
            subjectName = subjectName.substring(0, subjectName.lastIndexOf("_"));
        }
        int childSubjectId = -1;

        try {

            childSubjectId = getSubjectId(subjectName, subId, projectName, mappingManagerUtil);

        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (childSubjectId > 0) {
                sb.append("SubjectName : ").append(subjectName).append(" Already Available.\n\n");
            } else {

                Subject subjectDetails = new Subject();

                subjectDetails.setSubjectName(subjectName);
                if (StringUtils.isNotBlank(subjectDescription)) {
                    subjectDetails.setSubjectDescription(subjectDescription);
                } else {
                    subjectDetails.setSubjectDescription("Description");
                }

                AuditHistory auditHistory = new AuditHistory();
                auditHistory.setCreatedBy("Administrator");
                subjectDetails.setAuditHistory(auditHistory);
                subjectDetails.setProjectId(projectId);
                subjectDetails.setConsiderUserDefinedFlag("Y");
                if (subId > 0) {
                    subjectDetails.setParentSubjectId(subId);
                }

                try {
                    RequestStatus retRS = mappingManagerUtil.createSubject(subjectDetails);
                    sb.append(subjectName + " " + retRS.getStatusMessage() + "\n\n");

                } catch (Exception e) {

                    e.printStackTrace();
                }
            }
        } catch (Exception e) {

            SSISController.logger.info("methodName :--createChildSubject(-,-)");
            if (e.getMessage().equalsIgnoreCase("getAttribute: Session already invalidated")) {
                SSISController.logger.info("Session already invalidated");
            }

            SSISController.logger.error(e.getMessage());
            e.printStackTrace();
        }

        return sb.toString();
    }

    /**
     * this method will create subjects for the catOption Subject Name providing
     * in the subjectName cat option
     *
     * @param subjectName
     * @param mappingManagerUtil
     * @param projectId
     * @param projectName
     * @return subjectId
     */
    public int createSubjectForCatOption(String subjectName, MappingManagerUtil mappingManagerUtil, int projectId, String projectName) {

        String[] subjectArraySpilt = null;
        int catOptionSubjectId = 0;
        subjectName = FilenameUtils.normalizeNoEndSeparator(subjectName, true);

        if (subjectName.contains(",")) {
            subjectArraySpilt = subjectName.split(",");
        } else {
            subjectArraySpilt = subjectName.split("/");
        }

        int subjectCount = 0;
        int subId = 0;
        try {
            for (String subjectHierarchy : subjectArraySpilt) {
                if (subjectCount == 0 && !StringUtils.isBlank(subjectHierarchy)) {
                    createSubject(projectId, mappingManagerUtil, subjectHierarchy, projectName);
                    String removeSpecialCharsFromSubjectHierarchy = replaceSubjectNameSpecialCharactersWithUnderscore(subjectHierarchy);
                    subId = getSubjectId(removeSpecialCharsFromSubjectHierarchy, 0, projectName, mappingManagerUtil);
                } else {
                    if (!StringUtils.isBlank(subjectHierarchy)) {
                        createChildSubject(subjectHierarchy, projectId, subId, mappingManagerUtil, projectName, "");
                        String removeSpecialCharsFromSubjectHierarchy = replaceSubjectNameSpecialCharactersWithUnderscore(subjectHierarchy);

                        if (subId > 0) {
                            subId = getSubjectId(removeSpecialCharsFromSubjectHierarchy, subId, projectName, mappingManagerUtil);
                        }

                    }

                }
                if (!StringUtils.isBlank(subjectHierarchy)) {
                    subjectCount++;
                }

            }
        } catch (Exception e) {
            writeExeceptionLog(e, "createSubjectForCatOption(-,-)");

            e.printStackTrace();
        }
        catOptionSubjectId = subId;

        return catOptionSubjectId;
    }

    /**
     * this method creates subjects according to the file path hierarchy
     *
     * @param subjectName
     * @param mappingManagerUtil
     * @param projectId
     * @param projectName
     * @param catOptionSubjectId
     * @return
     */
    public int createFilePathSubjects(String subjectName, MappingManagerUtil mappingManagerUtil, int projectId, String projectName, int catOptionSubjectId) {
        int subjectId = 0;
        try {
            String[] subjectArraySpilt = null;
            globalSubjectHierarchy = "";

            subjectName = FilenameUtils.normalizeNoEndSeparator(subjectName, true);

            subjectArraySpilt = subjectName.split("/");

            int subjectCount = 0;
            int subId = 0;

            for (String subjectHierarchy : subjectArraySpilt) {
                if (StringUtils.isNotBlank(subjectHierarchy)
                        && (subjectHierarchy.contains(Constants.dtsx) || subjectHierarchy.contains(Constants.dtsx.toUpperCase()))) {

                    continue;

                }

                if (subjectCount == 0 && !StringUtils.isBlank(subjectHierarchy) && catOptionSubjectId <= 0) {
                    createSubject(projectId, mappingManagerUtil, subjectHierarchy, projectName);

                    subId = getSubjectId(subjectHierarchy, 0, projectName, mappingManagerUtil);
                } else if (subjectCount == 0 && !StringUtils.isBlank(subjectHierarchy) && catOptionSubjectId > 0) {
                    createChildSubject(subjectHierarchy, projectId, catOptionSubjectId, mappingManagerUtil, projectName, "");

                    subId = getSubjectId(subjectHierarchy, catOptionSubjectId, projectName, mappingManagerUtil);
                } else {
                    if (!StringUtils.isBlank(subjectHierarchy)) {
                        createChildSubject(subjectHierarchy, projectId, subId, mappingManagerUtil, projectName, "");

                        if (subId > 0) {
                            subId = getSubjectId(subjectHierarchy, subId, projectName, mappingManagerUtil);
                        }

                    }

                }
                if (StringUtils.isNotBlank(subjectHierarchy)) {
                    if (StringUtils.isBlank(globalSubjectHierarchy)) {
                        globalSubjectHierarchy = subjectHierarchy;
                    } else {
                        globalSubjectHierarchy = globalSubjectHierarchy + Delimiter.emm_Delimiter + subjectHierarchy;
                    }
                    subjectCount++;
                }

            }
            if (subId > 0) {
                subjectId = subId;
            } else {
                subjectId = catOptionSubjectId;
            }

        } catch (Exception e) {
            writeExeceptionLog(e, "createFilePathSubjects(-,-)");

            e.printStackTrace();
        }
        return subjectId;
    }

    /**
     * this method will return the subject id providing subjectName and
     * parendSubjectid or projectName is mandatory
     *
     * @param subjectName
     * @param parentSubjectId
     * @param projectName
     * @param mappingManagerUtil
     * @return subjectId
     */
    public int getSubjectId(String subjectName, int parentSubjectId, String projectName, MappingManagerUtil mappingManagerUtil) {

        int subjectId = 0;
        subjectName = subjectName.replaceAll(Constants.commonRegularExpression, "_");
        if (subjectName.endsWith("_")) {
            subjectName = subjectName.substring(0, subjectName.lastIndexOf("_"));
        }
        String key = "";
        if (parentSubjectId > 0) {
            key = projectName + Delimiter.delimiter + parentSubjectId + Delimiter.delimiter + subjectName;
        } else {
            key = projectName + Delimiter.delimiter + subjectName;
        }

        try {
            if (SSISController.getSubjectIdsMaps.get(key) != null) {
                subjectId = SSISController.getSubjectIdsMaps.get(key);
            } else {
                if (parentSubjectId > 0) {
                    subjectId = mappingManagerUtil.getSubjectId(parentSubjectId, Node.NodeType.MM_SUBJECT, subjectName);
                } else {
                    subjectId = mappingManagerUtil.getSubjectId(projectName, subjectName);
                }
            }
            if (subjectId > 0) {
                SSISController.getSubjectIdsMaps.put(key, subjectId);
            }

        } catch (Exception e) {
            writeExeceptionLog(e, "getSubjectId(-,-)");

            e.printStackTrace();
            subjectId = 0;
        }
        return subjectId;

    }

    /**
     * this method will take the subject name and replace special characters
     * with underscore if any.
     *
     * @param subjectName
     * @return subjectNameSpecisCharactersWithUnderscore
     */
    public static String replaceSubjectNameSpecialCharactersWithUnderscore(String subjectName) {
        try {
            subjectName = subjectName.replaceAll(Constants.commonRegularExpression, "_");
            if (subjectName.endsWith("_")) {
                subjectName = subjectName.substring(0, subjectName.lastIndexOf("_"));
            }
        } catch (Exception e) {
            writeExeceptionLog(e, "replaceSubjectNameSpecialCharactersWithUnderscore(-,-)");

            e.printStackTrace();
        }

        return subjectName;
    }

    /**
     * in this method we are creating mapping for the control flow,data flow and
     * query level mappings
     *
     *
     * @param subjectId
     * @param mapName
     * @param acpInputParameterBean
     * @param mappingSpecifications
     * @param mapping
     * @return mappingID along with mapping status
     */
    public static int createMappings(int subjectId, String mapName, ACPInputParameterBean acpInputParameterBean, ArrayList<MappingSpecificationRow> mappingSpecifications, Mapping mapping, String packageLog, StringBuilder logData) {

        int mappingID = 0;
        try {
            String loadType = acpInputParameterBean.getLoadType();
            MappingManagerUtil mappingManagerUtil = acpInputParameterBean.getMappingManagerUtil();
            KeyValueUtil keyValueUtil = acpInputParameterBean.getKeyValueUtil();
            RequestStatus status = null;

            int projectId = acpInputParameterBean.getProjectId();
            mappingID = MappingManagerUtilAutomation.getMappingId(subjectId, mapName, projectId, mappingManagerUtil);
            float mappingVersion = MappingManagerUtilAutomation.getTheMapCurrentVersion(mappingID, mappingManagerUtil);
            if (mappingID > 0 && "Delete/Reload".equalsIgnoreCase(loadType)) {

                try {
                    String deleteStatus = "";
                    if (subjectId > 0) {
                        deleteStatus = mappingManagerUtil.deleteMappingAs(subjectId, "MM_SUBJECT", mapName, "SPECIFIC_VERSION", mappingVersion, "json");
                    } else {
                        deleteStatus = mappingManagerUtil.deleteMappingAs(projectId, "MM_PROJECT", mapName, "SPECIFIC_VERSION", mappingVersion, "json");
                    }

                    logData.append(packageLog).append(deleteStatus).append(" with name ").append(mapName).append("\n");
                } catch (Exception e) {
                    writeExeceptionLog(e, "createMappings(-,-)");
                    e.printStackTrace();
                }
                try {
                    status = mappingManagerUtil.createMapping(mapping);

                    if (!status.isRequestSuccess()) {
                        mappingID = CreateMappingVersion.creatingMapVersionForIncremental(projectId, mapName, subjectId, mappingSpecifications, mappingManagerUtil, keyValueUtil);

                    } else {
                        mappingID = MappingManagerUtilAutomation.getMappingId(subjectId, mapName, projectId, mappingManagerUtil);
                    }
                } catch (Exception e) {
                    writeExeceptionLog(e, "createMappings(-,-)");
                    e.printStackTrace();
                }

            } else if (mappingID > 0 && loadType.equalsIgnoreCase("Archive/Reload")) {

                mappingID = CreateMappingVersion.creatingMapVersionForIncremental(projectId, mapName, subjectId, mappingSpecifications, mappingManagerUtil, keyValueUtil);

            } else {
                try {
                    status = mappingManagerUtil.createMapping(mapping, true);
                    logData.append(packageLog).append(status.getStatusMessage()).append(" with name ").append(mapName).append("\n");

                } catch (Exception e) {
                    writeExeceptionLog(e, "createMappings(-,-)");
                    e.printStackTrace();
                }
                mappingID = MappingManagerUtilAutomation.getMappingId(subjectId, mapName, projectId, mappingManagerUtil);

            }
        } catch (Exception e) {
            e.printStackTrace();
            writeExeceptionLog(e, "createMappings(-,-)");
        }
        return mappingID;
    }

    /**
     * Returns MappingManager version according to the running instance
     *
     * @return mmVersionf
     */
    public float getMappingManagerVersion() {

        float mmVersionf = 0;
        try {
            String mmVersion = ApplicationConstants.APPLICATION_VERSION_FULL;
            String[] mmVersionArray = mmVersion.split("\\.");
            int splitLength = mmVersionArray.length;
            if (splitLength >= 2) {
                mmVersion = mmVersionArray[0] + "." + mmVersionArray[1];
            }
            mmVersionf = Float.parseFloat(mmVersion);
        } catch (Exception e) {
            writeExeceptionLog(e, "getMappingManagerVersion(-,-)");
            e.printStackTrace();
            mmVersionf = 0;
        }
        return mmVersionf;
    }

    /**
     * this method will return mappingId back to the calling method providing
     * mappingManagerUtil,mapName and projectId or subjectId is mandatory
     *
     * @param subjectId
     * @param mapName
     * @param projectId
     * @param mappingManagerUtil
     * @return mappingId
     */
    public static int getMappingId(int subjectId, String mapName, int projectId, MappingManagerUtil mappingManagerUtil) {

        int mappingId = 0;
        try {
            if (subjectId > 0) {
                mappingId = mappingManagerUtil.getMappingId(subjectId, mapName, Node.NodeType.MM_SUBJECT);
            } else {
                mappingId = mappingManagerUtil.getMappingId(projectId, mapName, Node.NodeType.MM_PROJECT);

            }
        } catch (Exception e) {
            writeExeceptionLog(e, "getMappingId(-,-)");
            e.printStackTrace();
            mappingId = 0;
        }

        return mappingId;
    }

    /**
     * this method returns the mapping version for a given map Id
     *
     * @param mapingId
     * @param mappingManagerUtil
     * @return
     */
    public static float getTheMapCurrentVersion(int mapingId, MappingManagerUtil mappingManagerUtil) {
        float mappingVersion = 0.0f;

        try {
            Mapping mapObject = mappingManagerUtil.getMapping(mapingId);
            mappingVersion = mapObject.getMappingSpecVersion();
        } catch (Exception e) {
            writeExeceptionLog(e, "getMappingId(-,-)");
            e.printStackTrace();
            mappingVersion = 0.0f;
        }

        return mappingVersion;

    }

    /**
     * this method returns a list of key value updating the given extended
     * properties map
     *
     * @param extendedpropMap
     * @param extendedPropertiesdup
     * @return
     */
    public static List<KeyValue> getKeyValueMap(Map<String, String> extendedpropMap, Map<String, String> extendedPropertiesdup) {

        List<KeyValue> keyvaluesList = new ArrayList<>();
        try {

            for (Map.Entry<String, String> entry : extendedpropMap.entrySet()) {
                String key = entry.getKey();
                String uKey = key;
                if (key.split(Delimiter.ed_ge_Delimiter).length > 1) {
                    uKey = key.split(Delimiter.ed_ge_Delimiter)[1];
                }
                if (!StringUtils.isBlank(uKey)) {
                    uKey = uKey.trim();

                    if (uKey.contains("$")) {
                        uKey = uKey.substring(0, uKey.lastIndexOf("$"));

                    }

                }

                String value = entry.getValue();
                if (StringUtils.isBlank(value)) {
                    continue;
                }

                if (value.contains(Delimiter.delimiter)) {
                    value = value.split(Delimiter.delimiter)[0];
                }
                if (value.length() >= 4000) {
                    value = value.substring(0, 3995) + "...";
                }

                KeyValue keyValue = new KeyValue();
                keyValue.setKey(uKey);
                keyValue.setValue(value);
                keyValue.setVisibility(1);
                keyValue.setPublished(true);

                keyvaluesList.add(keyValue);
                uKey = uKey.replace("(", "").replace(")", "");

                extendedPropertiesdup.put(uKey, value);
            }

        } catch (Exception e) {
            e.printStackTrace();
            writeExeceptionLog(e, "getKeyValueMap(-,-)");

        }
        return keyvaluesList;
    }

    /**
     * this method replaces the double quotes present in the table name
     *
     * @param tableName
     * @return
     */
    public static String replaceDoubleQuotesWithEmptyFromTheTableName(String tableName) {

        try {
            tableName = tableName.replace("\"", "");
        } catch (Exception e) {
            e.printStackTrace();
            writeExeceptionLog(e, "replaceDoubleQuotesWithEmptyFromTheTableName(-,-)");
        }

        return tableName;

    }

    /**
     * this method returns the version build number present in the subject
     * description
     *
     * @param subjectDescription
     * @return
     */
    public static int getTheVersionBuildNumberFromSubjectDescription(String subjectDescription) {
        int dtsxBuildNumberFromSubject = 0;

        try {

            try {
                String subjectDescriptionArray[] = subjectDescription.split("::");

                int length = subjectDescriptionArray.length;
                if (length == 3) {
                    subjectDescription = subjectDescriptionArray[2];
                    dtsxBuildNumberFromSubject = Integer.parseInt(subjectDescription);
                }

            } catch (Exception e) {
                e.printStackTrace();
                writeExeceptionLog(e, "getTheVersionBuildNumberFromSubjectDescription(-,-)");
            }

        } catch (Exception e) {
            writeExeceptionLog(e, "getTheVersionBuildNumberFromSubjectDescription(-,-)");
            dtsxBuildNumberFromSubject = 0;
        }
        return dtsxBuildNumberFromSubject;

    }

    /**
     * this method updates the subject description with checking the file
     * whether its a ISPAC or DTSX
     *
     * @param packageNameSubjectId
     * @param dtsxPackageVersionBuildNumber
     * @param dtsxorIspac
     * @param inputFile
     * @param inputParameterBean
     * @return
     */
    public static int updateSubjectDescription(int packageNameSubjectId, int dtsxPackageVersionBuildNumber, String dtsxorIspac, File inputFile, ACPInputParameterBean inputParameterBean) {
        int dtsxBuildNumberFromSubject = 0;

        Subject subject1 = null;
        MappingManagerUtil mappingManagerUtil = inputParameterBean.getMappingManagerUtil();
        try {
            subject1 = mappingManagerUtil.getSubject(packageNameSubjectId);

            String subjectDescription = subject1.getSubjectDescription();
            dtsxBuildNumberFromSubject = getTheVersionBuildNumberFromSubjectDescription(subjectDescription);

            subjectDescription = "";

            String fileName = inputFile.getName();
            if (dtsxorIspac.equalsIgnoreCase("dtsx")) {
                subjectDescription = "PackageDeploymentModel::" + fileName + "::";
            } else {
                subjectDescription = dtsxorIspac + ".ispac ::" + fileName + "::";
            }
            subjectDescription = subjectDescription + dtsxPackageVersionBuildNumber;
            subject1.setSubjectDescription(subjectDescription);
            mappingManagerUtil.updateSubject(subject1);

        } catch (Exception e) {
            writeExeceptionLog(e, "updateSubjectDescription(-,-)");
            e.printStackTrace();
            dtsxBuildNumberFromSubject = 0;
        }
        return dtsxBuildNumberFromSubject;

    }

    /**
     *
     * @param e
     * @param methodName
     */
    public static void writeExeceptionLog(Exception e, String methodName) {
        SSISController.logger.info("methodName :--" + methodName);
        try {

            if ("getAttribute: Session already invalidated".equalsIgnoreCase(e.getMessage())) {
                SSISController.logger.info("Session already invalidated");
                SSISController.isSessionExpired = true;
            }
            SSISController.logger.error(e);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    /**
     *
     * @param subjectId
     * @param mappingManagerUtil
     * @return
     */
    public static String getDataflowSubjectDescription(int subjectId, MappingManagerUtil mappingManagerUtil) {

        String subjectDescription = "";
        try {
            Subject subject = mappingManagerUtil.getSubject(subjectId);
            subjectDescription = subject.getSubjectDescription();
        } catch (Exception e) {
            subjectDescription = "";
            e.printStackTrace();
        }
        return subjectDescription;
    }

    /**
     *
     * @param subjectId
     * @param mappingManagerUtil
     * @param subjectDescription
     */
    public static void updateSubjectDescriptionForDataflow(int subjectId, MappingManagerUtil mappingManagerUtil, String subjectDescription) {
        try {
            Subject subject = mappingManagerUtil.getSubject(subjectId);
            subject.setSubjectDescription(subjectDescription);
            mappingManagerUtil.updateSubject(subject);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * in this method we are deleting existing maps if those maps deactivated or
     * deleted through SSIS package.
     *
     * @param subjectMaps
     * @param activeMapsSets
     * @param mappingManagerUtil
     * @param subjectId
     * @param projectId
     * @param dataflowKeySetString
     * @return
     */
    public static String deleteDisledMap(String subjectMaps, Set<String> activeMapsSets, MappingManagerUtil mappingManagerUtil, int subjectId, int projectId, String dataflowKeySetString, Set<String> disbledSet, Set<String> deletedSet, String dfOrDfQueryMap) {
        try {
            String[] subjectMapsArray = subjectMaps.split("\n");
            for (String mapName : subjectMapsArray) {
                mapName = mapName.replace("<br>", "");
                if (!activeMapsSets.contains(mapName)) {
                    if (SSISController.disbaledAndDeletedDataFlows.containsKey(mapName) && "df".equalsIgnoreCase(dfOrDfQueryMap)) {
                        disbledSet.add(mapName + "__PKG__");
                        dataflowKeySetString = "<br>\n" + dataflowKeySetString + "<br>\n" + mapName + "_Disabled";
                    } else if ("df".equalsIgnoreCase(dfOrDfQueryMap)) {
                        deletedSet.add(mapName + "__PKG__");
                        dataflowKeySetString = "<br>\n" + dataflowKeySetString + "<br>\n" + mapName + "_Deleted";
                    }

                    if ("dfquery".equalsIgnoreCase(dfOrDfQueryMap) && disbledSet.contains(mapName.split("PKG__")[0] + "PKG__")) {
                        dataflowKeySetString = "<br>\n" + dataflowKeySetString + "<br>\n" + mapName + "_Disabled";
                    } else if ("dfquery".equalsIgnoreCase(dfOrDfQueryMap) && deletedSet.contains(mapName.split("PKG__")[0] + "PKG__")) {
                        dataflowKeySetString = "<br>\n" + dataflowKeySetString + "<br>\n" + mapName + "_Deleted";
                    }
                    mapName = mapName.replaceAll("[^a-zA-Z0-9 \\p{L}_-]", "_");
                    int mapId = getMappingId(subjectId, mapName, projectId, mappingManagerUtil);
                    if (mapId > 0) {
                        mappingManagerUtil.deleteMapping(mapId);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataflowKeySetString;
    }

    public static String returnBottomFourHeirarchyLevelSubjects(String hierarchyEnvironementName) {
        String[] hierarchyEnvironementNameArray = hierarchyEnvironementName.split(Delimiter.emm_Delimiter);
        try {
            int hierarchyLength = hierarchyEnvironementNameArray.length;
            if (hierarchyLength > 4) {
                hierarchyEnvironementName = hierarchyEnvironementNameArray[hierarchyLength - 4] + "__" + hierarchyEnvironementNameArray[hierarchyLength - 3] + "__" + hierarchyEnvironementNameArray[hierarchyLength - 2] + "__" + hierarchyEnvironementNameArray[hierarchyLength - 1];
            } else {
                hierarchyEnvironementName = hierarchyEnvironementName.replace(Delimiter.emm_Delimiter, "__");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hierarchyEnvironementName;
    }

    /**
     * this method will return the String without special characters
     *
     * @param name
     * @return
     */
    public static String returnAnyStringWithoutSPecialCharatcers(String name) {
        try {

            name = name.replaceAll(Constants.commonRegularExpression, "_");
            if (name.endsWith("_")) {
                name = name.substring(0, name.lastIndexOf("_"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return name;
    }
}
