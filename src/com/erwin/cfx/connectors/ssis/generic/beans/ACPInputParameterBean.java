/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.beans;

import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.w3c.dom.Document;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date:- 13-08-2021
 */
public class ACPInputParameterBean {

    @Override
    public String toString() {
        return "InputParameterBean{" + "supportFilePath=" + supportFilePath + ", projectName=" + projectName + ", subjectName=" + subjectName + ", loadType=" + loadType + ", parentAndChildRealtion=" + parentAndChildRealtion + ", fileTypeSystemName=" + fileTypeSystemName + ", fileTypeEnvironmentNameForCSV=" + fileTypeEnvironmentNameForCSV + ", fileTypeEnvironmentNameForXML=" + fileTypeEnvironmentNameForXML + ", fileTypeEnvironmentNameForJSON=" + fileTypeEnvironmentNameForJSON + ", fileTypeDefaultEnvironmentNameForExcel=" + fileTypeDefaultEnvironmentNameForExcel + ", isMetadaCreated=" + isMetadaCreated + ", isColumnLevelSyncup=" + isColumnLevelSyncup + ", sourceFileType=" + sourceFileType + ", defaultSchema=" + defaultSchema + ", metadataSyncupPath=" + metadataSyncupPath + ", fileType=" + fileType + ", uploadFilePath=" + uploadFilePath + ", textBoxFilePath=" + textBoxFilePath + ", postSyncUp=" + postSyncUp + ", sourceServerUdfNumber=" + sourceServerUdfNumber + ", sourceDatabaseUdfNumber=" + sourceDatabaseUdfNumber + ", targetServerUdfNumber=" + targetServerUdfNumber + ", targetDatabaseUdfNumber=" + targetDatabaseUdfNumber + ", sqlDatabaseType=" + sqlDatabaseType + ", controlflowMappingCheckFlag=" + controlflowMappingCheckFlag + ", dataFlowQueryMapCheckFlag=" + dataFlowQueryMapCheckFlag + ", storeProcedureTableCreation=" + storeProcedureTableCreation + ", metadataDefaultSystemCreation=" + metadataDefaultSystemCreation + ", logFlag=" + logFlag + ", isExecute2008Connector=" + isExecute2008Connector + ", defaultSystemName=" + defaultSystemName + ", defaultEnvrionmentName=" + defaultEnvrionmentName + ", logFilePath=" + logFilePath + ", archiveFilePath=" + archiveFilePath + ", dataSourceParamFilePath=" + dataSourceParamFilePath + ", deleteOrArchiveSourceFile=" + deleteOrArchiveSourceFile + ", mappingManagerUtil=" + mappingManagerUtil + ", systemManagerUtil=" + systemManagerUtil + ", keyValueUtil=" + keyValueUtil + '}';
    }

    private String supportFilePath;
    private String projectName;
    private String subjectName;
    private String loadType;
    private String parentAndChildRealtion;
    private String fileTypeSystemName;
    private String fileTypeEnvironmentNameForCSV;
    private String fileTypeEnvironmentNameForXML;
    private String fileTypeEnvironmentNameForJSON;
    private String fileTypeDefaultEnvironmentNameForExcel;
    private String isMetadaCreated;
    private String isColumnLevelSyncup;
    private String sourceFileType;
    private String defaultSchema;
    private String metadataSyncupPath;
    private String fileType;
    private String uploadFilePath;
    private String textBoxFilePath;
    private String postSyncUp;
    private String sourceServerUdfNumber;
    private String controlFlowQueriesPath;
    private String dataFlowQueriesPath;
    private String defaultDatabaseType;
    private Set<String> dbTypeSet;
    private Map ddlCacheMap;
    private String sourceDatabaseUdfNumber;
    private String targetServerUdfNumber;
    private String targetDatabaseUdfNumber;
    private String sqlDatabaseType;
    private String controlflowMappingCheckFlag;
    private boolean dataFlowQueryMapCheckFlag;
    private String storeProcedureTableCreation;
    private String inputFileName;
    private String metadataDefaultSystemCreation;
    private boolean logFlag;
    private boolean isExecute2008Connector;
    private String defaultSystemName;
    private String defaultEnvrionmentName;
    private String logFilePath;
    private String archiveFilePath;
    private String dataSourceParamFilePath;
    private String deleteOrArchiveSourceFile;
    private MappingManagerUtil mappingManagerUtil;
    private SystemManagerUtil systemManagerUtil;
    private File inputFile;
    private Document document;
    private Map<String, HashMap<String, HashMap<String, String>>> parentComponentAndChildPackageName;
    private Map<String, String> parameterHashMap;
    private Map<String, String> childLevelVaribleMap;
    private boolean childLevelPackageFlag;
    private Map<String, String> projectLevelVaraiblesMap;
    private Map<String, String> componentClassIdsMap;
    private int projectId;
    private Map metaDataCacheMap;
    private HashMap<String, String> allDBMap;
    private HashMap userDF;
    private List<String> fileExtensionList;
    private HashMap<String, String> openQueryHashMap;
    private String metadataCreatedFlag;
    private Map<String, String> connectionPropertiesHashMap;
    private String oDataEnvironmentName;
    private String syncupRules;
    private int maxLimit;
    private String envType;
    private boolean MapSyncupInstaedOfJsonFile;
    private boolean isVolumeSyncUp;
    private String ispacName;

    //isJsonEncryption  encryptionAndDecryptionKey
    private String jsonEncryption;
    private String encryptionAndDecryptionKey;

    public String getJsonEncryption() {
        return jsonEncryption;
    }

    public void setJsonEncryption(String jsonEncryption) {
        this.jsonEncryption = jsonEncryption;
    }

    public String getEncryptionAndDecryptionKey() {
        return encryptionAndDecryptionKey;
    }

    public void setEncryptionAndDecryptionKey(String encryptionAndDecryptionKey) {
        this.encryptionAndDecryptionKey = encryptionAndDecryptionKey;
    }

    public String getIspacName() {
        return ispacName;
    }

    public void setIspacName(String ispacName) {
        this.ispacName = ispacName;
    }

    public boolean isIsVolumeSyncUp() {
        return isVolumeSyncUp;
    }

    public void setIsVolumeSyncUp(boolean isVolumeSyncUp) {
        this.isVolumeSyncUp = isVolumeSyncUp;
    }

    public boolean isMapSyncupInstaedOfJsonFile() {
        return MapSyncupInstaedOfJsonFile;
    }

    public void setMapSyncupInstaedOfJsonFile(boolean MapSyncupInstaedOfJsonFile) {
        this.MapSyncupInstaedOfJsonFile = MapSyncupInstaedOfJsonFile;
    }

    public String getEnvType() {
        return envType;
    }

    public void setEnvType(String envType) {
        this.envType = envType;
    }

    public int getMaxLimit() {
        return maxLimit;
    }

    public void setMaxLimit(int maxLimit) {
        this.maxLimit = maxLimit;
    }

    public String getSyncupRules() {
        return syncupRules;
    }

    public void setSyncupRules(String syncupRules) {
        this.syncupRules = syncupRules;
    }

    public String getoDataEnvironmentName() {
        return oDataEnvironmentName;
    }

    public void setoDataEnvironmentName(String oDataEnvironmentName) {
        this.oDataEnvironmentName = oDataEnvironmentName;
    }

    public Map<String, String> getConnectionPropertiesHashMap() {
        return connectionPropertiesHashMap;
    }

    public void setConnectionPropertiesHashMap(Map<String, String> connectionPropertiesHashMap) {
        this.connectionPropertiesHashMap = connectionPropertiesHashMap;
    }

    public String getInputFileName() {
        return inputFileName;
    }

    public void setInputFileName(String inputFileName) {
        this.inputFileName = inputFileName;
    }

    public String getMetadataCreatedFlag() {
        return metadataCreatedFlag;
    }

    public void setMetadataCreatedFlag(String metadataCreatedFlag) {
        this.metadataCreatedFlag = metadataCreatedFlag;
    }

    public String getDefaultDatabaseType() {
        return defaultDatabaseType;
    }

    public void setDefaultDatabaseType(String defaultDatabaseType) {
        this.defaultDatabaseType = defaultDatabaseType;
    }

    public Set<String> getDbTypeSet() {
        return dbTypeSet;
    }

    public void setDbTypeSet(Set<String> dbTypeSet) {
        this.dbTypeSet = dbTypeSet;
    }

    public Map getDdlCacheMap() {
        return ddlCacheMap;
    }

    public void setDdlCacheMap(Map ddlCacheMap) {
        this.ddlCacheMap = ddlCacheMap;
    }

    private Map<String, String> projectLevelConnectionInfo;

    public Map<String, String> getProjectLevelConnectionInfo() {
        return projectLevelConnectionInfo;
    }

    public void setProjectLevelConnectionInfo(Map<String, String> projectLevelConnectionInfo) {
        this.projectLevelConnectionInfo = projectLevelConnectionInfo;
    }

    public String getControlFlowQueriesPath() {
        return controlFlowQueriesPath;
    }

    public void setControlFlowQueriesPath(String controlFlowQueriesPath) {
        this.controlFlowQueriesPath = controlFlowQueriesPath;
    }

    public String getDataFlowQueriesPath() {
        return dataFlowQueriesPath;
    }

    public void setDataFlowQueriesPath(String dataFlowQueriesPath) {
        this.dataFlowQueriesPath = dataFlowQueriesPath;
    }

    public String getMetadataDefaultSystemCreation() {
        return metadataDefaultSystemCreation;
    }

    public void setMetadataDefaultSystemCreation(String metadataDefaultSystemCreation) {
        this.metadataDefaultSystemCreation = metadataDefaultSystemCreation;
    }

    public String getStoreProcedureTableCreation() {
        return storeProcedureTableCreation;
    }

    public void setStoreProcedureTableCreation(String storeProcedureTableCreation) {
        this.storeProcedureTableCreation = storeProcedureTableCreation;
    }

    public HashMap<String, String> getOpenQueryHashMap() {
        return openQueryHashMap;
    }

    public void setOpenQueryHashMap(HashMap<String, String> openQueryHashMap) {
        this.openQueryHashMap = openQueryHashMap;
    }

    public List<String> getFileExtensionList() {
        return fileExtensionList;
    }

    public void setFileExtensionList(List<String> fileExtensionList) {
        this.fileExtensionList = fileExtensionList;
    }

    public HashMap getUserDF() {
        return userDF;
    }

    public void setUserDF(HashMap userDF) {
        this.userDF = userDF;
    }

    private String tableClassCaption;

    public String getTableClassCaption() {
        return tableClassCaption;
    }

    public void setTableClassCaption(String tableClassCaption) {
        this.tableClassCaption = tableClassCaption;
    }

    public HashMap<String, String> getAllDBMap() {
        return allDBMap;
    }

    public void setAllDBMap(HashMap<String, String> allDBMap) {
        this.allDBMap = allDBMap;
    }

    public Map getMetaDataCacheMap() {
        return metaDataCacheMap;
    }

    public void setMetaDataCacheMap(Map metaDataCacheMap) {
        this.metaDataCacheMap = metaDataCacheMap;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public Map<String, String> getComponentClassIdsMap() {
        return componentClassIdsMap;
    }

    public void setComponentClassIdsMap(Map<String, String> componentClassIdsMap) {
        this.componentClassIdsMap = componentClassIdsMap;
    }

    public Map<String, String> getProjectLevelVaraiblesMap() {
        return projectLevelVaraiblesMap;
    }

    public void setProjectLevelVaraiblesMap(Map<String, String> projectLevelVaraiblesMap) {
        this.projectLevelVaraiblesMap = projectLevelVaraiblesMap;
    }

    public Map<String, HashMap<String, HashMap<String, String>>> getParentComponentAndChildPackageName() {
        return parentComponentAndChildPackageName;
    }

    public void setParentComponentAndChildPackageName(Map<String, HashMap<String, HashMap<String, String>>> parentComponentAndChildPackageName) {
        this.parentComponentAndChildPackageName = parentComponentAndChildPackageName;
    }

    public Map<String, String> getParameterHashMap() {
        return parameterHashMap;
    }

    public void setParameterHashMap(Map<String, String> parameterHashMap) {
        this.parameterHashMap = parameterHashMap;
    }

    public Map<String, String> getChildLevelVaribleMap() {
        return childLevelVaribleMap;
    }

    public void setChildLevelVaribleMap(Map<String, String> childLevelVaribleMap) {
        this.childLevelVaribleMap = childLevelVaribleMap;
    }

    public boolean isChildLevelPackageFlag() {
        return childLevelPackageFlag;
    }

    public void setChildLevelPackageFlag(boolean childLevelPackageFlag) {
        this.childLevelPackageFlag = childLevelPackageFlag;
    }

    public File getInputFile() {
        return inputFile;
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public MappingManagerUtil getMappingManagerUtil() {
        return mappingManagerUtil;
    }

    public void setMappingManagerUtil(MappingManagerUtil mappingManagerUtil) {
        this.mappingManagerUtil = mappingManagerUtil;
    }

    public SystemManagerUtil getSystemManagerUtil() {
        return systemManagerUtil;
    }

    public void setSystemManagerUtil(SystemManagerUtil systemManagerUtil) {
        this.systemManagerUtil = systemManagerUtil;
    }

    public KeyValueUtil getKeyValueUtil() {
        return keyValueUtil;
    }

    public void setKeyValueUtil(KeyValueUtil keyValueUtil) {
        this.keyValueUtil = keyValueUtil;
    }
    KeyValueUtil keyValueUtil;

    public String getDeleteOrArchiveSourceFile() {
        return deleteOrArchiveSourceFile;
    }

    public void setDeleteOrArchiveSourceFile(String deleteOrArchiveSourceFile) {
        this.deleteOrArchiveSourceFile = deleteOrArchiveSourceFile;
    }

    public String getSubjectName() {
        return subjectName;
    }

    public void setSubjectName(String subjectName) {
        this.subjectName = subjectName;
    }

    public String getDefaultSystemName() {
        return defaultSystemName;
    }

    public void setDefaultSystemName(String defaultSystemName) {
        this.defaultSystemName = defaultSystemName;
    }

    public String getDefaultEnvrionmentName() {
        return defaultEnvrionmentName;
    }

    public void setDefaultEnvrionmentName(String defaultEnvrionmentName) {
        this.defaultEnvrionmentName = defaultEnvrionmentName;
    }

    public String getLogFilePath() {
        return logFilePath;
    }

    public void setLogFilePath(String logFilePath) {
        this.logFilePath = logFilePath;
    }

    public String getArchiveFilePath() {
        return archiveFilePath;
    }

    public void setArchiveFilePath(String archiveFilePath) {
        this.archiveFilePath = archiveFilePath;
    }

    public String getDataSourceParamFilePath() {
        return dataSourceParamFilePath;
    }

    public void setDataSourceParamFilePath(String dataSourceParamFilePath) {
        this.dataSourceParamFilePath = dataSourceParamFilePath;
    }

    public String getSupportFilePath() {
        return supportFilePath;
    }

    public void setSupportFilePath(String supportFilePath) {
        this.supportFilePath = supportFilePath;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getSourceServerUdfNumber() {
        return sourceServerUdfNumber;
    }

    public void setSourceServerUdfNumber(String sourceServerUdfNumber) {
        this.sourceServerUdfNumber = sourceServerUdfNumber;
    }

    public String getSourceDatabaseUdfNumber() {
        return sourceDatabaseUdfNumber;
    }

    public void setSourceDatabaseUdfNumber(String sourceDatabaseUdfNumber) {
        this.sourceDatabaseUdfNumber = sourceDatabaseUdfNumber;
    }

    public String getTargetServerUdfNumber() {
        return targetServerUdfNumber;
    }

    public void setTargetServerUdfNumber(String targetServerUdfNumber) {
        this.targetServerUdfNumber = targetServerUdfNumber;
    }

    public String getTargetDatabaseUdfNumber() {
        return targetDatabaseUdfNumber;
    }

    public void setTargetDatabaseUdfNumber(String targetDatabaseUdfNumber) {
        this.targetDatabaseUdfNumber = targetDatabaseUdfNumber;
    }

    public String getSqlDatabaseType() {
        return sqlDatabaseType;
    }

    public void setSqlDatabaseType(String sqlDatabaseType) {
        this.sqlDatabaseType = sqlDatabaseType;
    }

    public String isControlflowMappingCheckFlag() {
        return controlflowMappingCheckFlag;
    }

    public void setControlflowMappingCheckFlag(String controlflowMappingCheckFlag) {
        this.controlflowMappingCheckFlag = controlflowMappingCheckFlag;
    }

    public boolean isDataFlowQueryMapCheckFlag() {
        return dataFlowQueryMapCheckFlag;
    }

    public void setDataFlowQueryMapCheckFlag(boolean dataFlowQueryMapCheckFlag) {
        this.dataFlowQueryMapCheckFlag = dataFlowQueryMapCheckFlag;
    }

    public boolean isLogFlag() {
        return logFlag;
    }

    public void setLogFlag(boolean logFlag) {
        this.logFlag = logFlag;
    }

    public boolean isIsExecute2008Connector() {
        return isExecute2008Connector;
    }

    public void setIsExecute2008Connector(boolean isExecute2008Connector) {
        this.isExecute2008Connector = isExecute2008Connector;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getLoadType() {
        return loadType;
    }

    public void setLoadType(String loadType) {
        this.loadType = loadType;
    }

    public String getParentAndChildRealtion() {
        return parentAndChildRealtion;
    }

    public void setParentAndChildRealtion(String parentAndChildRealtion) {
        this.parentAndChildRealtion = parentAndChildRealtion;
    }

    public String getFileTypeSystemName() {
        return fileTypeSystemName;
    }

    public void setFileTypeSystemName(String fileTypeSystemName) {
        this.fileTypeSystemName = fileTypeSystemName;
    }

    public String getFileTypeEnvironmentNameForCSV() {
        return fileTypeEnvironmentNameForCSV;
    }

    public void setFileTypeEnvironmentNameForCSV(String fileTypeEnvironmentNameForCSV) {
        this.fileTypeEnvironmentNameForCSV = fileTypeEnvironmentNameForCSV;
    }

    public String getFileTypeEnvironmentNameForXML() {
        return fileTypeEnvironmentNameForXML;
    }

    public void setFileTypeEnvironmentNameForXML(String fileTypeEnvironmentNameForXML) {
        this.fileTypeEnvironmentNameForXML = fileTypeEnvironmentNameForXML;
    }

    public String getFileTypeEnvironmentNameForJSON() {
        return fileTypeEnvironmentNameForJSON;
    }

    public void setFileTypeEnvironmentNameForJSON(String fileTypeEnvironmentNameForJSON) {
        this.fileTypeEnvironmentNameForJSON = fileTypeEnvironmentNameForJSON;
    }

    public String getFileTypeDefaultEnvironmentNameForExcel() {
        return fileTypeDefaultEnvironmentNameForExcel;
    }

    public void setFileTypeDefaultEnvironmentNameForExcel(String fileTypeDefaultEnvironmentNameForExcel) {
        this.fileTypeDefaultEnvironmentNameForExcel = fileTypeDefaultEnvironmentNameForExcel;
    }

    public String getIsMetadaCreated() {
        return isMetadaCreated;
    }

    public void setIsMetadaCreated(String isMetadaCreated) {
        this.isMetadaCreated = isMetadaCreated;
    }

    public String getIsColumnLevelSyncup() {
        return isColumnLevelSyncup;
    }

    public void setIsColumnLevelSyncup(String isColumnLevelSyncup) {
        this.isColumnLevelSyncup = isColumnLevelSyncup;
    }

    public String getSourceFileType() {
        return sourceFileType;
    }

    public void setSourceFileType(String sourceFileType) {
        this.sourceFileType = sourceFileType;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public String getMetadataSyncupPath() {
        return metadataSyncupPath;
    }

    public void setMetadataSyncupPath(String metadataSyncupPath) {
        this.metadataSyncupPath = metadataSyncupPath;
    }

    public String getUploadFilePath() {
        return uploadFilePath;
    }

    public void setUploadFilePath(String uploadFilePath) {
        this.uploadFilePath = uploadFilePath;
    }

    public String getTextBoxFilePath() {
        return textBoxFilePath;
    }

    public void setTextBoxFilePath(String textBoxFilePath) {
        this.textBoxFilePath = textBoxFilePath;
    }

    public String getPostSyncUp() {
        return postSyncUp;
    }

    public void setPostSyncUp(String postSyncUp) {
        this.postSyncUp = postSyncUp;
    }

}
