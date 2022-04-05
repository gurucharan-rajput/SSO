/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.controller;

import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.cfx.connectors.json.syncup.SyncupWithServerDBSchamaSysEnvCPT;
import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.io.FileProperties;
import com.erwin.cfx.connectors.ssis.generic.io.XMLUtil;
import com.erwin.cfx.connectors.ssis.generic.service.common.CreateControlFlowMaps;
import com.erwin.cfx.connectors.ssis.generic.service.common.CreateDataFlowMappings;
import com.erwin.cfx.connectors.ssis.generic.service.common.CreateQueryMappings;
import com.erwin.cfx.connectors.ssis.generic.service.ssis2008.SSIS2008;
import com.erwin.cfx.connectors.ssis.generic.service.ssis2014.SSIS2014;
import com.erwin.cfx.connectors.ssis.generic.util.Constants;
import com.erwin.cfx.connectors.ssis.generic.util.MappingManagerUtilAutomation;
import com.erwin.cfx.connectors.ssis.generic.util.variablereplacement.DynamicVaribleValueReplacement;
import com.erwin.cfx.connectors.ssis.generic.io.ZipTheArchiveDirectory;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.codehaus.plexus.util.StringUtils;
import org.w3c.dom.Document;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 13-08-2021
 */
@SuppressWarnings("all")
public class SSISController {

    public static Logger logger = Logger.getLogger(SSISController.class.getName());
    static MappingManagerUtilAutomation mappingManagerUtilAutomation;
    FileProperties fileProperties;
    static int projectId = 0;
    static int catOptionSubjectId = 0;
    static String archiveFilePath = "";
    static String inputFilePath = "";
    static String fileType = "";
    static MappingManagerUtil mappingManagerUtil = null;
    public static SystemManagerUtil systemManagerUtil = null;
    static KeyValueUtil keyValueUtil = null;
    static String projectName = "";
    static Set<Map<String, String>> controlFlowSet = new HashSet<>();
    static Set<Map<String, String>> dataFlowSet = new HashSet<>();
    static String fileDate = "";
    static final String dataFormatString = "yyyy-MM-dd_HH:mm:ss";
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dataFormatString);
    public static Map<String, HashMap<String, HashMap<String, String>>> parentComponentAndChildPackageNameMap = new HashMap();
    public static Map<String, String> parameterHashMap = new HashMap<>();
    public static boolean is2014 = false;

    public static HashMap metadatChacheHM = new HashMap();

    public static HashMap<String, String> allTablesMap = new HashMap<>();

    public static boolean isIspacDtsxFileFlag = false;
    public static boolean isSessionExpired = false;
    public static String ispacFileName = "";

    public static StringBuilder status = new StringBuilder();

    public static Map<String, Integer> getSubjectIdsMaps = new HashMap<>();
    public static Map<String, String> disbaledAndDeletedDataFlows = new HashMap<>();
    
    
    

    /**
     * this methods take the inputs from the ACP and forward to appropriate
     * classes and methods for their execution
     *
     * @param acpInputParameterBean
     *
     * @return the entire connector execution log back to the acp.
     */
    public String processTheSSISExecution(ACPInputParameterBean acpInputParameterBean) {
        long startTime = System.currentTimeMillis();
        String supportFilePath = "";
        projectName = "";
        String subjectName = "";
        fileType = "";
        String uploadFilePath = "";
        String textBoxFilePath = "";
        archiveFilePath = "";
        String deleteOrArchiveSourceFile = "";
        projectId = 0;
        catOptionSubjectId = 0;
        simpleDateFormat = new SimpleDateFormat(dataFormatString);
        fileDate = "";
        fileDate = simpleDateFormat.format(new Date());
        fileDate = fileDate.replace(":", "-");
        String jsonFilePath = "";
        String dbType = "";
        String defaultDatabaseType = "";
        metadatChacheHM = new HashMap();
        String logFile = "";
        ispacFileName = "";
        getSubjectIdsMaps = new HashMap<>();
        disbaledAndDeletedDataFlows = new HashMap<>();
        isSessionExpired = false;

        if (acpInputParameterBean != null) {
            supportFilePath = acpInputParameterBean.getSupportFilePath();
            projectName = acpInputParameterBean.getProjectName();
            subjectName = acpInputParameterBean.getSubjectName();
            fileType = acpInputParameterBean.getFileType();
            uploadFilePath = acpInputParameterBean.getUploadFilePath();
            textBoxFilePath = acpInputParameterBean.getTextBoxFilePath();
            archiveFilePath = acpInputParameterBean.getArchiveFilePath();
            mappingManagerUtil = acpInputParameterBean.getMappingManagerUtil();
            systemManagerUtil = acpInputParameterBean.getSystemManagerUtil();
            keyValueUtil = acpInputParameterBean.getKeyValueUtil();
            deleteOrArchiveSourceFile = acpInputParameterBean.getDeleteOrArchiveSourceFile();

            jsonFilePath = acpInputParameterBean.getMetadataSyncupPath();
            dbType = acpInputParameterBean.getSqlDatabaseType();
            defaultDatabaseType = acpInputParameterBean.getDefaultDatabaseType();
            logFile = acpInputParameterBean.getLogFilePath();
        } else {
            acpInputParameterBean = new ACPInputParameterBean();
        }
        int maxLimit = acpInputParameterBean.getMaxLimit();

        String envType = acpInputParameterBean.getEnvType();
        boolean isMapSyncupInstaedOfJsonFile = false;
        boolean isVolumeSyncup = acpInputParameterBean.isIsVolumeSyncUp();

//if (isVolumeSyncup) {
//            metadatChacheHM = com.erwin.cfx.connectors.json.syncup.v1.SyncupWithServerDBSchamaSysEnvCPT.createVolumeHashMaps(systemManagerUtil, maxLimit, metadatChacheHM, envType);
//            isMapSyncupInstaedOfJsonFile = com.erwin.cfx.connectors.json.syncup.v1.SyncupWithServerDBSchamaSysEnvCPT.isMapSyncupInstaedOfJsonFile;
//        }
        acpInputParameterBean.setMapSyncupInstaedOfJsonFile(isMapSyncupInstaedOfJsonFile);
        logFile = FilenameUtils.normalizeNoEndSeparator(logFile, true) + "/";
        String log4jPath = logFile + "SSIS RE log" + "_" + fileDate + ".log";
        FileAppender fileAppender = null;
        try {
            fileAppender = new FileAppender(new SimpleLayout(), log4jPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (StringUtils.isBlank(dbType) && StringUtils.isNotBlank(defaultDatabaseType)) {
            dbType = defaultDatabaseType;
        }
        Set<String> dbTypeSet = new HashSet<>();

        String[] dbTypeArray = dbType.split(",");
        Arrays.stream(dbTypeArray).forEachOrdered((str) -> dbTypeSet.add(str));
        acpInputParameterBean.setDbTypeSet(dbTypeSet);
        if (!isMapSyncupInstaedOfJsonFile) {
            HashMap<String, String> allDBMap = SyncupWithServerDBSchamaSysEnvCPT.getMap(jsonFilePath, "Databases");
            acpInputParameterBean.setAllDBMap(allDBMap);
        }

        acpInputParameterBean.setMetaDataCacheMap(metadatChacheHM);

        mappingManagerUtilAutomation = new MappingManagerUtilAutomation();
        fileProperties = new FileProperties();

        projectId = mappingManagerUtilAutomation.returnTheProjectId(projectName, mappingManagerUtil);
        acpInputParameterBean.setProjectId(projectId);

        catOptionSubjectId = 0;
        if (StringUtils.isNotBlank(subjectName)) {
            catOptionSubjectId = mappingManagerUtilAutomation.createSubjectForCatOption(subjectName, mappingManagerUtil, projectId, projectName);
        }

        mappingManagerUtilAutomation = new MappingManagerUtilAutomation();
        float mappingVersion = mappingManagerUtilAutomation.getMappingManagerVersion();
        supportFilePath = fileProperties.getSupportFilesPath(supportFilePath, mappingVersion);
        String controlFlowQueriesPath = supportFilePath + "/CONTROLFLOWQUERIES";
        String dataFlowQueriesPath = supportFilePath + "/DATAFLOWQUERIES";
        acpInputParameterBean.setControlFlowQueriesPath(controlFlowQueriesPath);
        acpInputParameterBean.setDataFlowQueriesPath(dataFlowQueriesPath);
        String compClassPropertyFilePath = supportFilePath + "/PropertyFiles/ComponentClassIds.properties";
        Map<String, String> componentClassIdsMap = fileProperties.buildPropertyFileMap(compClassPropertyFilePath);
        inputFilePath = fileProperties.getInputFilePath(fileType, uploadFilePath, textBoxFilePath, mappingVersion);

        String logHeaderData = fileProperties.createLogHeaderWithCatParams(inputFilePath, projectId, acpInputParameterBean);
//        Logger.getRootLogger().addAppender(fileAppender);

        logger.removeAllAppenders();
        logger.addAppender(fileAppender);
        logger.setAdditivity(false);
        logger.setLevel(Level.DEBUG);
        logger.info(logHeaderData);

        HashMap<String, String> userDefinedMap = fileProperties.prepareUserDefinedMap(acpInputParameterBean);
        acpInputParameterBean.setUserDF(userDefinedMap);
        acpInputParameterBean.setComponentClassIdsMap(componentClassIdsMap);
        String extensionsPropertyFilePath = supportFilePath + "/PropertyFiles/ExtensionFile.properties";
        List<String> fileExtensions = fileProperties.readFileExtensionData(extensionsPropertyFilePath);
        acpInputParameterBean.setFileExtensionList(fileExtensions);
        String dataSourceParamFile = acpInputParameterBean.getDataSourceParamFilePath();
        XMLUtil.prepareConnectionPropertiesMapFromFile(dataSourceParamFile, acpInputParameterBean);

        if ("Archive source File".equalsIgnoreCase(deleteOrArchiveSourceFile) && "FilePath".equals(fileType)) {
            String ssisBackUpFileName = "SSIS_Backup_" + fileDate;
            ZipTheArchiveDirectory.zipTheSourceFolder(inputFilePath, archiveFilePath, ssisBackUpFileName);
        }
        iterateInputFiles(inputFilePath, deleteOrArchiveSourceFile, acpInputParameterBean);
        try {

            if ("UploadFiles".equalsIgnoreCase(fileType)) {
                File inputFile = new File(inputFilePath);
                FileUtils.cleanDirectory(inputFile);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        long milliseconds = endTime - startTime;
        long minutes = TimeUnit.MILLISECONDS.toMinutes(milliseconds);
        String formatTme = String.format("%d Milliseconds = %d minutes\n", milliseconds, minutes);

//        System.out.println("Total Time Taken To Execute The Connector Is---" + formatTme);
        logger.info("Total Time Taken To Execute The Connector Is---" + formatTme);

        return status.toString();
    }

    /**
     *
     * this method takes inputFilepath as input and iterate each file to process
     * the execution
     *
     * @param inputfFilePath
     * @param deleteOrArchiveSourceFile
     * @param inputParameterBean
     */
    public void iterateInputFiles(String inputfFilePath, String deleteOrArchiveSourceFile, ACPInputParameterBean inputParameterBean) {

        List<String> directoriesList = new ArrayList<>();
        try {
            File ssisInputFilePath = new File(inputfFilePath);
            File[] ssisFiles = ssisInputFilePath.listFiles();
            for (File ssisFile : ssisFiles) {
                if (isSessionExpired) {
                    break;
                }

                ispacFileName = "";
                isIspacDtsxFileFlag = false;
                Map<String, String> projectLevelConnectionInfo = new HashMap<>();
                Map<String, String> projectLevelVariableInfo = new HashMap<>();
                if (ssisFile.isDirectory()) {
                    directoriesList.add(ssisFile.getAbsolutePath());
                    continue;
                }

                String ssisFileName = ssisFile.getName();
                String ssisFilePath = ssisFile.getAbsolutePath();

                if (ssisFileName.endsWith(".ispac") || ssisFileName.endsWith(".zip")) {
                    isIspacDtsxFileFlag = true;
                    String zipDir = ssisFile.getParent();
                    String zipFileName = ssisFileName.replace(".ispac", "").replace(".zip", "");
                    ispacFileName = zipFileName;
                    zipDir = FilenameUtils.normalizeNoEndSeparator(zipDir, true);
                    File destDir = new File(zipDir + "/" + zipFileName + "/");
                    Map<String, List<String>> filesFromIspacExport = fileProperties.unzipTheIspacFiles(ssisFilePath, destDir.getAbsolutePath());

                    logger.info(" \n\n ##### Starts processing Ispac file ====> " + zipFileName + " #####  \n");
                    logger.info(" \nIspac file processing ==>" + zipFileName + "==>" + simpleDateFormat.format(new Date()) + "\n");

                    List<String> dtsxFiles = filesFromIspacExport.get("dtsx");
                    List<String> dependencyFiles = filesFromIspacExport.get("dependency");

                    dependencyFiles.forEach(eachFile -> {
                        File inputFile = new File(eachFile);
                        if (eachFile.endsWith(".conmgr")) {
                            XMLUtil.getProjectLevelConnectionInfo(inputFile, inputParameterBean, projectLevelConnectionInfo);
                        }
                        if (eachFile.endsWith(".params")) {
                            XMLUtil.getProjectLevelParamFileData(inputFile, inputParameterBean, projectLevelVariableInfo);
                        }
                    });
                    dtsxFiles.forEach(filePath
                            -> {
                        processIndividualFile(zipFileName, filePath, inputParameterBean, deleteOrArchiveSourceFile);
                    });
                    try {
                        if (!"Keep".equalsIgnoreCase(deleteOrArchiveSourceFile) && "FilePath".equals(fileType)) {

                            FileUtils.deleteQuietly(ssisFile);
                        }
                        FileUtils.deleteDirectory(destDir);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    logger.info(" \n\n ##### Completed processing Ispac file ====>" + ispacFileName + "====>" + simpleDateFormat.format(new Date()) + " ##### \n ");

                } else {
                    if (ssisFilePath.endsWith("dtsx") || ssisFilePath.endsWith("DTSX")) {
                        isIspacDtsxFileFlag = false;

                        processIndividualFile(Constants.dtsx, ssisFilePath, inputParameterBean, deleteOrArchiveSourceFile);

                    }

                }
            }

            if (!directoriesList.isEmpty()) {
                directoriesList.forEach(subDirectory -> {
                    iterateInputFiles(subDirectory, deleteOrArchiveSourceFile, inputParameterBean);
                });

            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "iterateInputFiles(-,-)");
        }
    }

    /**
     * this method will take individual DTSX file as an input and starts
     * processing that file.
     *
     * @param dtsxorIspac
     * @param individualFilePath
     * @param inputParameterBean
     */
    public static void processIndividualFile(String dtsxorIspac, String individualFilePath, ACPInputParameterBean inputParameterBean, String deleteOrArchiveSourceFile) {

        String subjectStatus = "";
        is2014 = false;
        String loadType = inputParameterBean.getLoadType();
        inputParameterBean.setIspacName(ispacFileName);
        

        String hierarchyEnvironementName = "";
        clearStaticVaraibles();
        try {
            individualFilePath = FilenameUtils.normalizeNoEndSeparator(individualFilePath, true);

            inputParameterBean.setParentComponentAndChildPackageName(parentComponentAndChildPackageNameMap);
            inputParameterBean.setParameterHashMap(parameterHashMap);
            File inputFile = new File(individualFilePath);
            String tempIndividualFilePath = individualFilePath;

            int subjectId = 0;
            int packageNameSubjectId = 0;
            String dtsxFileName = "";
            dtsxFileName = inputFile.getName();
            dtsxFileName = dtsxFileName.replace(".dtsx", "").replace(".DTSX", "");
            dtsxFileName = MappingManagerUtilAutomation.returnAnyStringWithoutSPecialCharatcers(dtsxFileName);

            inputParameterBean.setInputFileName(dtsxFileName);
            if ("FilePath".equalsIgnoreCase(fileType) && (individualFilePath.contains("." + Constants.dtsx) || individualFilePath.contains("." + Constants.dtsx.toUpperCase()))) {

                individualFilePath = individualFilePath.replace(inputFilePath, "");

                String ispacWithDtsxName = "/" + dtsxorIspac + "/" + dtsxFileName + ".";
                if (isIspacDtsxFileFlag) {
                    individualFilePath = individualFilePath.replace(ispacWithDtsxName, "/");
                }
                subjectId = mappingManagerUtilAutomation.createFilePathSubjects(individualFilePath, mappingManagerUtil, projectId, projectName, catOptionSubjectId);
                hierarchyEnvironementName = MappingManagerUtilAutomation.globalSubjectHierarchy;
            } else {
                subjectId = catOptionSubjectId;
            }

            if (tempIndividualFilePath.contains("." + Constants.dtsx) || tempIndividualFilePath.contains("." + Constants.dtsx.toUpperCase())) {

                if (!"dtsx".equalsIgnoreCase(dtsxorIspac)) {
                    if (StringUtils.isBlank(hierarchyEnvironementName)) {
                        hierarchyEnvironementName = dtsxorIspac;
                    } else {
                        hierarchyEnvironementName = hierarchyEnvironementName + Delimiter.emm_Delimiter + dtsxorIspac;
                    }

                    if (subjectId > 0) {
                        mappingManagerUtilAutomation.createChildSubject(dtsxorIspac, projectId, subjectId, mappingManagerUtil, projectName, "");
                    } else {
                        mappingManagerUtilAutomation.createChildSubject(dtsxorIspac, projectId, 0, mappingManagerUtil, projectName, "");
                    }
                    subjectId = mappingManagerUtilAutomation.getSubjectId(dtsxorIspac, subjectId, projectName, mappingManagerUtil);
                }

                hierarchyEnvironementName = hierarchyEnvironementName.replaceAll(Constants.commonRegularExpression, "_");
                if (hierarchyEnvironementName.endsWith(Delimiter.emm_Delimiter)) {
                    hierarchyEnvironementName = hierarchyEnvironementName.substring(0, hierarchyEnvironementName.lastIndexOf(Delimiter.emm_Delimiter));
                }
                hierarchyEnvironementName = MappingManagerUtilAutomation.returnBottomFourHeirarchyLevelSubjects(hierarchyEnvironementName);
                subjectStatus = mappingManagerUtilAutomation.createChildSubject(dtsxFileName, projectId, subjectId, mappingManagerUtil, projectName, "");

                packageNameSubjectId = mappingManagerUtilAutomation.getSubjectId(dtsxFileName, subjectId, projectName, mappingManagerUtil);

            }

            if (isIspacDtsxFileFlag) {

                logger.info(" \n\n @@@@@  Starts processing dtsx Package    ====> " + dtsxFileName + " @@@@@ \n");
                logger.info("Ispac file " + dtsxorIspac + "==>" + "Package : " + dtsxFileName + "==>" + simpleDateFormat.format(new Date()) + "\n\n");

                logger.info("Ispac file " + dtsxorIspac + "==>" + "Package : " + dtsxFileName + " :- " + subjectStatus);
            } else {
                logger.info(" \n\n #####  Starts processing dtsx Package   ====> " + dtsxFileName + " ##### \n");
                logger.info("dtsx Package Processing : " + "==>" + dtsxFileName + " :- " + subjectStatus);
            }

            Document xmlDocument = XMLUtil.returnTheDocument(inputFile);

            int packageFormatVersionNumber = XMLUtil.getPackageFormatVersionFromDtsxfile(xmlDocument);

            int dtsxPackageVersionBuildNumber = 0;

            if (packageFormatVersionNumber != 3) {
                dtsxPackageVersionBuildNumber = XMLUtil.getPackageBuildNumberFromDTSXFile_2014(xmlDocument);
            } else if (packageFormatVersionNumber == 3) {
                dtsxPackageVersionBuildNumber = XMLUtil.getPackageBuildNumberFromDTSXFile_2008(xmlDocument);
            }

            inputParameterBean.setInputFile(inputFile);
            inputParameterBean.setDocument(xmlDocument);
            int dtsxBuildNumberFromSubject = MappingManagerUtilAutomation.updateSubjectDescription(packageNameSubjectId, dtsxPackageVersionBuildNumber, dtsxorIspac, inputFile, inputParameterBean);
            if (!("Archive/Reload".equalsIgnoreCase(loadType)
                    && dtsxPackageVersionBuildNumber == dtsxBuildNumberFromSubject)) {

                Map<String, Object> controlFlowAndDataFlowLineageMaps = null;
                SSIS2014 ssis2014 = new SSIS2014();
                SSIS2008 ssis2008 = new SSIS2008();

                if (packageFormatVersionNumber > 3) {
                    is2014 = true;
                    controlFlowAndDataFlowLineageMaps = ssis2014.startExecution(inputParameterBean);

                } else {
                    is2014 = false;
                    controlFlowAndDataFlowLineageMaps = ssis2008.startExecution(inputParameterBean);
                }
                if (controlFlowAndDataFlowLineageMaps != null) {
                    createMappings(controlFlowAndDataFlowLineageMaps, packageNameSubjectId, inputParameterBean, dtsxFileName, hierarchyEnvironementName);
                }
                String parentAndChildRelation = inputParameterBean.getParentAndChildRealtion();
                parentComponentAndChildPackageNameMap = inputParameterBean.getParentComponentAndChildPackageName();
                parameterHashMap = inputParameterBean.getParameterHashMap();
                if ("true".equalsIgnoreCase(parentAndChildRelation) && parentComponentAndChildPackageNameMap != null && !parentComponentAndChildPackageNameMap.isEmpty()) {
                    processParentAndChildRelation(parentComponentAndChildPackageNameMap, inputFile, subjectId, dtsxFileName, inputParameterBean, hierarchyEnvironementName, loadType, dtsxorIspac);
                }

            }
            logger.info("\nFile Path of '" + dtsxFileName + "' : " + inputFile.getAbsolutePath());
            logger.info("\n\ndtsx Package Processing : '" + dtsxFileName + "' Execution End : \n\n *********************\n\n");

            logger.info(" \n\n dtsx Package Processing  completed ====>" + dtsxFileName + "====>" + simpleDateFormat.format(new Date()) + "\n");

            if (!isIspacDtsxFileFlag) {
                logger.info(" \n ##### Completed processing Dtsx file ====> " + dtsxFileName + " #####\n ");
            } else {
                logger.info(" \n @@@@@ Completed processing Dtsx file ====> " + dtsxFileName + " @@@@@\n ");
            }
            try {
                if (!"Keep".equalsIgnoreCase(deleteOrArchiveSourceFile) && "FilePath".equals(fileType)) {
                    FileUtils.deleteQuietly(inputFile);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "processIndividualFile(-,-)");
        }

    }

    /**
     * this method process the parent and child relation
     *
     * @param parentComponentAndChildPackageNameMap
     * @param inputFile
     * @param parentDirSubjId
     * @param dtsxFileName
     *
     * @param inputParameterBean
     * @param hierarchyEnvironementName
     * @param loadType
     * @param dtsxorIspac
     */
    public static void processParentAndChildRelation(Map<String, HashMap<String, HashMap<String, String>>> parentComponentAndChildPackageNameMap, File inputFile, int parentDirSubjId, String dtsxFileName, ACPInputParameterBean inputParameterBean, String hierarchyEnvironementName, String loadType, String dtsxorIspac) {

        try {
            SSIS2014 ssis2014 = new SSIS2014();
            SSIS2008 ssis2008 = new SSIS2008();
            parentComponentAndChildPackageNameMap.forEach((key, value) -> {
                controlFlowSet = new HashSet<>();
                dataFlowSet = new HashSet<>();
                disbaledAndDeletedDataFlows = new HashMap<>();
                String parentComponentName = key;
                boolean childLevelPackageFlag = true;
                inputParameterBean.setChildLevelPackageFlag(childLevelPackageFlag);
                HashMap<String, HashMap<String, String>> packageNameAndVaribleMap = value;
                String childPackageName = "";
                HashMap<String, String> varibleMap = new HashMap<>();
                for (Map.Entry<String, HashMap<String, String>> childPackageEntry1 : packageNameAndVaribleMap.entrySet()) {
                    childPackageName = childPackageEntry1.getKey();
                    varibleMap = childPackageEntry1.getValue();
                }
                String childSubjectName = "";

                String parentPath = inputFile.getParent();
                parentPath = FilenameUtils.normalizeNoEndSeparator(parentPath, true);
                String rootDirectory = parentPath + "/" + childPackageName;
                File inputFile1 = new File(rootDirectory);
                if (!inputFile1.exists()) {
                    childPackageName = childPackageName.replace(" ", "%20");
                    String parentPath1 = inputFile1.getParent();
                    parentPath1 = FilenameUtils.normalizeNoEndSeparator(parentPath1, true);
                    rootDirectory = parentPath1 + "/" + childPackageName;
                    inputFile1 = new File(rootDirectory);
                }
                if (inputFile1.exists()) {
                    childSubjectName = childPackageName.replace(".dtsx", "").replace(".DTSX", "");
                    int subjectId = 0;

                    mappingManagerUtilAutomation.createChildSubject(childSubjectName, projectId, parentDirSubjId, mappingManagerUtil, projectName, "");

                    subjectId = mappingManagerUtilAutomation.getSubjectId(childSubjectName, parentDirSubjId, projectName, mappingManagerUtil);

                    if (parentComponentName.length() >= 150) {
                        parentComponentName = parentComponentName.substring(0, 145) + "...";
                    }

                    // creating subject for parentPackageName Under the child package
                    mappingManagerUtilAutomation.createChildSubject(dtsxFileName, projectId, subjectId, mappingManagerUtil, projectName, "");

                    subjectId = mappingManagerUtilAutomation.getSubjectId(dtsxFileName, subjectId, projectName, mappingManagerUtil);

                    // creating subject for parentComponent Under the child package under parentpackageName subject
                    mappingManagerUtilAutomation.createChildSubject(parentComponentName, projectId, subjectId, mappingManagerUtil, projectName, "");

                    subjectId = mappingManagerUtilAutomation.getSubjectId(parentComponentName, subjectId, projectName, mappingManagerUtil);

                    HashMap<String, String> childLevelVaribleMap = DynamicVaribleValueReplacement.prepareNewchildLevelVaribleMapWithVariableAndParameterHashMap(varibleMap, parameterHashMap, parentComponentName);

                    inputParameterBean.setChildLevelVaribleMap(childLevelVaribleMap);
                    Document xmlDocument = XMLUtil.returnTheDocument(inputFile1);
                    inputParameterBean.setInputFile(inputFile1);
                    inputParameterBean.setDocument(xmlDocument);
                    Map<String, Object> controlFlowAndDataFlowLineageMaps = null;

//                    int packageFormatVersionNumber = XMLUtil.getPackageFormatVersionFromDtsxfile(xmlDocument);
//
//                    int dtsxPackageVersionBuildNumber = 0;
//                    if (packageFormatVersionNumber != 3) {
//                        dtsxPackageVersionBuildNumber = XMLUtil.getPackageBuildNumberFromDTSXFile_2014(xmlDocument);
//                    } else if (packageFormatVersionNumber == 3) {
//                        dtsxPackageVersionBuildNumber = XMLUtil.getPackageBuildNumberFromDTSXFile_2008(xmlDocument);
//                    }
//                    int dtsxBuildNumberFromSubject = MappingManagerUtilAutomation.updateSubjectDescription(subjectId, dtsxPackageVersionBuildNumber, dtsxorIspac, inputFile, inputParameterBean);
//                    if (!("incremental".equalsIgnoreCase(loadType)
//                            && dtsxPackageVersionBuildNumber == dtsxBuildNumberFromSubject)) {
                    if (is2014) {

                        controlFlowAndDataFlowLineageMaps = ssis2014.startExecution(inputParameterBean);

                    } else {

                        controlFlowAndDataFlowLineageMaps = ssis2008.startExecution(inputParameterBean);
                    }
                    createMappings(controlFlowAndDataFlowLineageMaps, subjectId, inputParameterBean, childSubjectName, hierarchyEnvironementName);
//                    }

                }

            });
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "processParentAndChildRelation(-,-)");
        }
    }

    /**
     * this method creates mappings for control flow,data flow and query level
     * mappings
     *
     * @param controlFlowAndDataFlowLineageMaps
     * @param subjectId
     * @param inputParameterBean
     * @param dtsxFileName
     * @param hierarchyEnvironementName
     */
    public static void createMappings(Map<String, Object> controlFlowAndDataFlowLineageMaps, int subjectId, ACPInputParameterBean inputParameterBean, String dtsxFileName, String hierarchyEnvironementName) {
        Map<String, List<HashMap<String, String>>> controlFlowLevelLineageMap = null;
        Map<String, List<HashMap<String, String>>> dataFlowLevelLineageMap = null;
        try {
            if (controlFlowAndDataFlowLineageMaps != null && controlFlowAndDataFlowLineageMaps.get("CONTROLFLOWMAPPINGS") != null) {
                controlFlowLevelLineageMap = (Map< String, List< HashMap< String, String>>>) controlFlowAndDataFlowLineageMaps.get("CONTROLFLOWMAPPINGS");
            }
            if (controlFlowAndDataFlowLineageMaps != null && controlFlowAndDataFlowLineageMaps.get("dataflowLineage") != null) {
                dataFlowLevelLineageMap = (Map< String, List< HashMap< String, String>>>) controlFlowAndDataFlowLineageMaps.get("dataflowLineage");
            }
            int controlFlowSubjectId = 0;
            String packageLog = "";
            if (!isIspacDtsxFileFlag) {
                packageLog = "dtsx Package Processing  : " + "==>" + dtsxFileName + " :- ";
            } else {
                packageLog = "Ispac file " + ispacFileName + "==>" + "Package : " + dtsxFileName + " :- ";
            }
            final String finalPackageLog = packageLog;

            if (controlFlowLevelLineageMap != null) {
                String controlFlowSubjectName = "ControlFlow";
                String subjectStatus = mappingManagerUtilAutomation.createChildSubject(controlFlowSubjectName, projectId, subjectId, mappingManagerUtil, projectName, "");
                controlFlowSubjectId = mappingManagerUtilAutomation.getSubjectId(controlFlowSubjectName, subjectId, projectName, mappingManagerUtil);

                String packageLogWithStatus = packageLog + subjectStatus;
                logger.info(packageLogWithStatus);
                String controlFlowStatus = CreateControlFlowMaps.createControlFlowLevelMaps(controlFlowLevelLineageMap, controlFlowSubjectId, projectId, inputParameterBean, controlFlowSet, packageLog, hierarchyEnvironementName);
                logger.info(controlFlowStatus);
                status.append(controlFlowStatus).append("\n\n");
            }
            int dataFlowSubjectId = 0;
            Set<String> disbledSet = new HashSet<>();
            Set<String> deletedSet = new HashSet<>();
            if (dataFlowLevelLineageMap != null) {
                String dataFlowSubjectName = "DataFlow";
                Set<String> dataflowKeySet = dataFlowLevelLineageMap.keySet();
                String dataflowKeySetString = "";
                if (dataflowKeySet != null) {
                    dataflowKeySetString = String.join("\n<br>", dataflowKeySet);
                }

                String subjectStatus = mappingManagerUtilAutomation.createChildSubject(dataFlowSubjectName, projectId, subjectId, mappingManagerUtil, projectName, dataflowKeySetString);
                dataFlowSubjectId = mappingManagerUtilAutomation.getSubjectId(dataFlowSubjectName, subjectId, projectName, mappingManagerUtil);
                String subjecDescriptionFromEMMSubject = MappingManagerUtilAutomation.getDataflowSubjectDescription(dataFlowSubjectId, mappingManagerUtil);

                dataflowKeySetString = MappingManagerUtilAutomation.deleteDisledMap(subjecDescriptionFromEMMSubject, dataflowKeySet, mappingManagerUtil, dataFlowSubjectId, projectId, dataflowKeySetString, disbledSet, deletedSet, "df");
                String packageLogWithStatus = packageLog + subjectStatus;
                logger.info(packageLogWithStatus);

                String dataFlowStatus = CreateDataFlowMappings.createDataFlowMappings(dataFlowLevelLineageMap, inputParameterBean, projectId, dataFlowSubjectId, dataFlowSet, packageLog, hierarchyEnvironementName);
                MappingManagerUtilAutomation.updateSubjectDescriptionForDataflow(dataFlowSubjectId, mappingManagerUtil, dataflowKeySetString);
                logger.info(dataFlowStatus);
                status.append(dataFlowStatus).append("\n\n");

            }
            String controlFlowQueriesPath = inputParameterBean.getControlFlowQueriesPath();
            String dataFlowQueriesPath = inputParameterBean.getDataFlowQueriesPath();
            List<String> queryFilePths = null;
            if (!controlFlowSet.isEmpty()) {
                String controlFlowQueriesFolderName = "ControlFlow_Queries";
                String subjectStatus = mappingManagerUtilAutomation.createChildSubject(controlFlowQueriesFolderName, projectId, controlFlowSubjectId, mappingManagerUtil, projectName, "");
                int controlFlowQuerySubjectId = mappingManagerUtilAutomation.getSubjectId(controlFlowQueriesFolderName, controlFlowSubjectId, projectName, mappingManagerUtil);
                String packageLogWithStatus = packageLog + subjectStatus;
                logger.info(packageLogWithStatus);
                queryFilePths = FileProperties.createQuaries(controlFlowSet, controlFlowQueriesPath);

                queryFilePths.forEach(queryFilePath -> {

                    String queryMappingstatus = CreateQueryMappings.createQueryLevelMappings(queryFilePath, projectId, controlFlowQuerySubjectId, dtsxFileName, inputParameterBean, "ControlFlow", finalPackageLog, new HashSet(), hierarchyEnvironementName);
                    logger.info(queryMappingstatus);

                    status.append(queryMappingstatus).append("\n\n");
                });
                FileProperties.cleanQueryDirectory(controlFlowQueriesPath);

            }
            if (!dataFlowSet.isEmpty()) {
                String dataFlowQueriesFolderName = "DataFlow_Queries";
                String subjectStatus = mappingManagerUtilAutomation.createChildSubject(dataFlowQueriesFolderName, projectId, dataFlowSubjectId, mappingManagerUtil, projectName, "");
                int dataFlowQuerySubjectId = mappingManagerUtilAutomation.getSubjectId(dataFlowQueriesFolderName, dataFlowSubjectId, projectName, mappingManagerUtil);
                queryFilePths = FileProperties.createQuaries(dataFlowSet, dataFlowQueriesPath);
                String packageLogWithStatus = packageLog + subjectStatus;
                Set<String> queryMapSet = new HashSet<>();
                logger.info(packageLogWithStatus);
                queryFilePths.forEach(queryFilePath -> {
                    String queryMappingstatus = CreateQueryMappings.createQueryLevelMappings(queryFilePath, projectId, dataFlowQuerySubjectId, dtsxFileName, inputParameterBean, "DataFlow", finalPackageLog, queryMapSet, hierarchyEnvironementName);
                    logger.info(queryMappingstatus);
                    status.append(queryMappingstatus).append("\n\n");
                });
                String dataflowKeySetString = "";

                dataflowKeySetString = String.join("\n<br>", queryMapSet);

                String subjecDescriptionFromEMMSubject = MappingManagerUtilAutomation.getDataflowSubjectDescription(dataFlowQuerySubjectId, mappingManagerUtil);

                dataflowKeySetString = MappingManagerUtilAutomation.deleteDisledMap(subjecDescriptionFromEMMSubject, queryMapSet, mappingManagerUtil, dataFlowQuerySubjectId, projectId, dataflowKeySetString, disbledSet, deletedSet, "dfquery");

                MappingManagerUtilAutomation.updateSubjectDescriptionForDataflow(dataFlowQuerySubjectId, mappingManagerUtil, dataflowKeySetString);
                FileProperties.cleanQueryDirectory(dataFlowQueriesPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "createMappings(-,-)");

        }

    }

    /**
     * this method clears the memory for static variables
     */
    public static void clearStaticVaraibles() {
        parentComponentAndChildPackageNameMap = new HashMap<>();
        parameterHashMap = new HashMap<>();
        controlFlowSet = new HashSet<>();
        dataFlowSet = new HashSet<>();
        disbaledAndDeletedDataFlows = new HashMap<>();

    }
}
