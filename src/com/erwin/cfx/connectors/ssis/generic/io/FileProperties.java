/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.io;

import com.ads.api.beans.common.APIConstants;
import com.erwin.cfx.connectors.sqlparser.v3.GetUserDefinedIds;
import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.controller.SSISController;
import com.icc.util.ADSConnectionPool;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 16-08-2021
 */
public class FileProperties {

    public int fileCount = 0;

    /**
     * Returns the supportFilesPath for the specified MappingManager
     * version(mmVersionf).
     *
     * @param supportFilesPath String
     * @param mmVersionf Float
     * @return String supportFilesPath
     */
    public String getSupportFilesPath(String supportFilesPath, float mmVersionf) {

        try {
            if (mmVersionf >= 10.1 && !supportFilesPath.contains(":")) {
                supportFilesPath = APIConstants.APPLICATION_DOCUMENTS_PATH + supportFilesPath;
            } else {
                supportFilesPath = supportFilesPath.replace("CATFX", "/CATFX/").replace("CATS", "CATS/").replace("Files", "/Files");
            }
            supportFilesPath = FilenameUtils.normalizeNoEndSeparator(supportFilesPath, true);
        } catch (Exception e) {
            SSISController.logger.info("methodName :--getSupportFilesPath(-,-)");
            SSISController.logger.error(e.getMessage());
            e.printStackTrace();
        }
        return supportFilesPath;
    }

    /**
     * reads the componentId properties file and make the componentId as key and
     * componentName as value preparing a LinkedHashMap
     *
     * @param propertyFilePath String
     * @return componentId's map
     */
    public Map<String, String> buildPropertyFileMap(String propertyFilePath) {

        Map<String, String> propertyFilesMap = new LinkedHashMap<>();
        Properties prop = new Properties();
        try (FileInputStream input = new FileInputStream(propertyFilePath)) {
            prop.load(input);
            Enumeration<?> e = prop.propertyNames();
            while (e.hasMoreElements()) {
                String key = (String) e.nextElement();
                String value = prop.getProperty(key);
                propertyFilesMap.put(key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--buildPropertyFileMap(-,-)");
            SSISController.logger.error(e.getMessage());
        }
        return propertyFilesMap;
    }

    /**
     * Returns file path depending on either upload or file path cat option
     *
     * @param filesType
     * @param uploadFilePath
     * @param textBoxFilePath
     * @param mmVersionf
     * @return inputFilePath
     */
    public String getInputFilePath(String filesType, String uploadFilePath, String textBoxFilePath, float mmVersionf) {
        String ssisfile = "";
        try {
            if ("UploadFiles".equalsIgnoreCase(filesType)) {
                JSONArray jsonarray = new JSONArray(uploadFilePath);
                for (int k1 = 0; k1 < jsonarray.length(); k1++) {
                    JSONObject jsonobject = jsonarray.getJSONObject(k1);
                    String serverName = jsonobject.getString("serverName");
                    if (mmVersionf >= 10.1) {
                        serverName = APIConstants.APPLICATION_DOCUMENTS_PATH + serverName;
                    }
                    serverName = serverName.substring(0, serverName.lastIndexOf("/"));
                    ssisfile = serverName;
                }
            } else {
                textBoxFilePath = FilenameUtils.normalizeNoEndSeparator(textBoxFilePath, true);
                if (textBoxFilePath.endsWith("/")) {
                    textBoxFilePath = textBoxFilePath.substring(0, textBoxFilePath.lastIndexOf("/"));
                }
                ssisfile = textBoxFilePath;
            }
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--getInputFilePath(-,-)");
            SSISController.logger.error(e.getMessage());
        }
        ssisfile = FilenameUtils.normalizeNoEndSeparator(ssisfile, true);
        return ssisfile;
    }

    /**
     * Returns a List of unzipped file paths
     *
     * @param zipFilePath
     * @param destDir
     * @return
     */
    public Map<String, List<String>> unzipTheIspacFiles(String zipFilePath, String destDir) {
        Map<String, List<String>> files = new HashMap<>();
        List<String> dtsxFiles = new ArrayList<>();
        List<String> dependencyFiles = new ArrayList<>();
        File dir = new File(destDir);
        // create output directory if it doesn't exist
        if (!dir.exists()) {
            dir.mkdirs();
        }
        FileInputStream fis;
        //buffer for read and write data to file
        byte[] buffer = new byte[1024];
        try {
            fis = new FileInputStream(zipFilePath);
            try (ZipInputStream zis = new ZipInputStream(fis)) {
                ZipEntry ze = zis.getNextEntry();

                while (ze != null) {
                    String fileName = ze.getName();
                    String fileAbsolutePath = destDir + File.separator + fileName;

                    File newFile = new File(fileAbsolutePath);
                    if (!newFile.exists()) {
                        fileName = fileName.replace("%20", " ");
                        fileAbsolutePath = destDir + File.separator + fileName;
                        newFile = new File(fileAbsolutePath);
                    }

                    //create directories for sub directories in zip
                    new File(newFile.getParent()).mkdirs();
                    try (FileOutputStream fos = new FileOutputStream(newFile)) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                    //close this ZipEntry
                    zis.closeEntry();
                    ze = zis.getNextEntry();
                    if (fileAbsolutePath.endsWith("dtsx")) {
                        dtsxFiles.add(fileAbsolutePath);
                    } else {
                        dependencyFiles.add(fileAbsolutePath);
                    }
                }
                //close last ZipEntry
                zis.closeEntry();
            }
            fis.close();
            files.put("dtsx", dtsxFiles);
            files.put("dependency", dependencyFiles);

        } catch (IOException e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--unzipTheIspacFiles(-,-)");
            SSISController.logger.error(e.getMessage());
        }
        return files;
    }

    /**
     * This method creates a hash map containing the user defined fields *
     * allocated with particular server and database details of source and
     * target
     *
     * @param acpInputParameterBean
     * @return
     */
    public HashMap<String, String> prepareUserDefinedMap(ACPInputParameterBean acpInputParameterBean) {
        HashMap<String, String> userDefined = new HashMap<>();
        try {
            String postSyncup = acpInputParameterBean.getPostSyncUp();
            String srcServer = acpInputParameterBean.getSourceServerUdfNumber();
            String srcDB = acpInputParameterBean.getSourceDatabaseUdfNumber();
            String tgtServer = acpInputParameterBean.getTargetServerUdfNumber();
            String tgtDB = acpInputParameterBean.getTargetDatabaseUdfNumber();
            if (StringUtils.isNotBlank(srcServer)) {
                userDefined.put("srcServer", srcServer);
            }
            if (StringUtils.isNotBlank(srcDB)) {
                userDefined.put("srcDB", srcDB);
            }
            if (StringUtils.isNotBlank(tgtServer)) {
                userDefined.put("tgtServer", tgtServer);
            }
            if (StringUtils.isNotBlank(tgtDB)) {
                userDefined.put("tgtDB", tgtDB);
            }
            Connection connection = ADSConnectionPool.getConnection();
            GetUserDefinedIds getUserDefinedFileds = new GetUserDefinedIds();
            if ("udf".equals(postSyncup)) {
                getUserDefinedFileds.getIdsFromNames(connection, userDefined);
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--prepareUserDefinedMap(-,-)");
            SSISController.logger.error(e.getMessage());
        }
        return userDefined;
    }

    /**
     * this method creates a query directory and writes the query into SQL file
     * returning the list of queries path
     *
     * @param querySet
     * @param queryOutputDirectory
     * @return
     */
    public static List<String> createQuaries(Set<Map<String, String>> querySet, String queryOutputDirectory) {
        try {

            File fw = new File(queryOutputDirectory);
            if (!fw.exists()) {
                fw.mkdir();
            } else {
                FileUtils.cleanDirectory(new File(queryOutputDirectory));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            SSISController.logger.info("methodName :--createQuaries(-,-)");
            SSISController.logger.error(ex.getMessage());
        }
        LinkedHashMap<String, String> queriesMap = new LinkedHashMap<>();
        String queryCompName = "";
        for (Map<String, String> map : querySet) {
            for (Map.Entry<String, String> entrySet : map.entrySet()) {
                String key = entrySet.getKey();

                if (key.contains("_ED_GE_")) {
                    queryCompName = key.replace("_ED_GE_queryName", "");
                    queryCompName = queryCompName.replaceAll("[^\\w\\s\\p{L}\\._-]+", "_");
                } else if (key.contains("$")) {

                    queryCompName = key.substring(0, key.lastIndexOf("$"));

                    queryCompName = queryCompName.replaceAll("[^\\w\\s\\p{L}\\._-]+", "_");
                }
                String value = entrySet.getValue();
                if (value.startsWith("WITH CTE AS")) {
                    value = value.substring(value.indexOf("WITH CTE AS") + 13, value.lastIndexOf(")")).trim() + ";";
                }
                if (value.toUpperCase().startsWith("DELETE")) {
                    continue;
                }
                if (queryCompName.length() >= 201) {
                    queryCompName = queryCompName.substring(0, 199);
                }

                if (key.trim().contains("$")) {
                    queriesMap.put(queryCompName, value + ";\n");
                }
            }
        }

        List<String> mappings = new ArrayList<>();
        for (Map.Entry<String, String> entry : queriesMap.entrySet()) {
            String key = entry.getKey();
            key = key.replace("\n", "").replaceAll("[^a-zA-Z0-9 \\p{L}_-]", "_");
            String value = entry.getValue();
            String fileName = queryOutputDirectory + "\\" + key;

            fileName = FilenameUtils.normalizeNoEndSeparator(fileName, true);

            File file = new File(fileName + ".sql");
            try {

                if (value.trim().equalsIgnoreCase("Select Getdate() as ProcessDateTime;")) {
                    continue;
                }
                if (!file.exists()) {
                    file.createNewFile();
                }
                if (file.canWrite()) {
                    value = value.replace("FOR FETCH ONLY", " ");
                    FileUtils.writeStringToFile(file, value, "UTF-8");
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                SSISController.logger.info("methodName :--createQuaries(-,-)");
                SSISController.logger.error(ex.getMessage());

            }

            mappings.add(fileName);
        }
        return mappings;
    }

    /**
     * this method will prepare the cat options info into the log file
     *
     * @param inputFilePath
     * @param projectId
     * @param acpInputParameterBean
     * @return
     */
    public String createLogHeaderWithCatParams(String inputFilePath, int projectId, ACPInputParameterBean acpInputParameterBean) {
        StringBuilder headerSB = new StringBuilder();
        try {
            String logHeader = "\n#############################################################\n" + "################# SSIS Reverse Engineer Log #################\n"
                    + "#############################################################\n\n";
            headerSB.append(logHeader);
            headerSB.append("Input Parameter ::ProjecName='" + acpInputParameterBean.getProjectName() + "'\n");
            headerSB.append("Input Parameter ::projectId='" + projectId + "'\n");
            headerSB.append("Input Parameter ::Subject Name='" + acpInputParameterBean.getSubjectName() + "'\n");
            headerSB.append("Input Parameter ::Syncup Rules='" + acpInputParameterBean.getSyncupRules() + "'\n");
            headerSB.append("Input Parameter ::Version Control='" + acpInputParameterBean.getLoadType() + "'\n");
            headerSB.append("Input Parameter ::Default System Name='" + acpInputParameterBean.getDefaultSystemName() + "'\n");
            headerSB.append("Input Parameter ::Default Environment Name='" + acpInputParameterBean.getDefaultEnvrionmentName() + "'\n");
            headerSB.append("Input Parameter ::Default Schema Name='" + acpInputParameterBean.getDefaultSchema() + "'\n");
            headerSB.append("Input Parameter ::SQL Type ='" + acpInputParameterBean.getDefaultDatabaseType() + "'\n");
            headerSB.append("Input Parameter ::Delete/Archive Source File='" + acpInputParameterBean.getDeleteOrArchiveSourceFile() + "'\n");
            headerSB.append("Input Parameter ::Harvest Metadata From Files='" + acpInputParameterBean.getMetadataDefaultSystemCreation() + "'\n");
            headerSB.append("Input Parameter ::Sync Source / Target Metadata='" + acpInputParameterBean.getIsColumnLevelSyncup() + "'\n");
            headerSB.append("Input Parameter ::Parent And Child Relation='" + acpInputParameterBean.getParentAndChildRealtion() + "'\n");
            headerSB.append("Input Parameter ::Control Flow Maps='" + acpInputParameterBean.isControlflowMappingCheckFlag() + "'\n");
            String filesType = acpInputParameterBean.getFileType();
            headerSB.append("Input Parameter ::Input File Option='" + filesType + "'\n");
            if (filesType.equals("UploadFiles")) {
                headerSB.append("Input Parameter ::Input SSIS Files Upload='" + inputFilePath + "'\n");
            } else {
                headerSB.append("Input Parameter ::Input SSIS Files Directory Path='" + inputFilePath + "'\n");
            }
            headerSB.append("Input Parameter ::Metadata='" + acpInputParameterBean.getMetadataSyncupPath() + "'\n");
            headerSB.append("Input Parameter ::Log='" + acpInputParameterBean.getLogFilePath() + "'\n");
            headerSB.append("Input Parameter ::ArchivePath='" + acpInputParameterBean.getArchiveFilePath() + "'\n");
            headerSB.append("Input Parameter ::Connection Properties FilePath='" + acpInputParameterBean.getDataSourceParamFilePath() + "'\n");
            headerSB.append("Input Parameter ::DB Type='" + acpInputParameterBean.getSqlDatabaseType() + "'\n");
            headerSB.append("\n\n=============================================================================================================\n\n");
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--createLogHeaderWithCatParams(-,-)");
            SSISController.logger.error(e.getMessage());
            headerSB.append(e);
        }
        return headerSB.toString();
    }

    /**
     * this method creates a list containing all the types of file extensions
     *
     * @param filePath
     * @return
     */
    public List<String> readFileExtensionData(String filePath) {

        List<String> fileExtensionList = new ArrayList<>();
        try {
            filePath = FilenameUtils.normalizeNoEndSeparator(filePath, true);
            String propertyFileContent = "";
            File file = new File(filePath);
            if (file.exists() && file.canRead()) {
                propertyFileContent = FileUtils.readFileToString(file, "utf-8");
                String[] splitPropertyFileContent = propertyFileContent.split(",");
                for (String fileExtension : splitPropertyFileContent) {
                    if (StringUtils.isNotBlank(fileExtension)) {
                        fileExtension = fileExtension.trim().toLowerCase();
                        fileExtensionList.add(fileExtension);
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            SSISController.logger.info("methodName :--readFileExtensionData(-,-)");
            SSISController.logger.error(ex.getMessage());
        }
        return fileExtensionList;
    }

    /**
     * this method clears the queries present in the given query path
     *
     * @param queriesPath
     */
    public static void cleanQueryDirectory(String queriesPath) {
        try {
            FileUtils.cleanDirectory(new File(queriesPath));
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--cleanQueryDirectory(-,-)");
            SSISController.logger.error(e.getMessage());
        }
    }
}
