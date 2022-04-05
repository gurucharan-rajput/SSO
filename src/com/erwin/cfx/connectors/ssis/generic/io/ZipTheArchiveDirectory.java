package com.erwin.cfx.connectors.ssis.generic.io;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import com.erwin.cfx.connectors.ssis.generic.controller.SSISController;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date:- 13-08-2021
 */
public class ZipTheArchiveDirectory {

    /**
     * creates a zip file in archive directory by collecting all the files
     * present in source directory
     *
     * @param sourcePath
     * @param archivePath
     * @param folderName
     */
    public static void zipTheSourceFolder(String sourcePath, String archivePath, String folderName) {
        try {
            sourcePath = FilenameUtils.normalizeNoEndSeparator(sourcePath, true);
            archivePath = FilenameUtils.normalizeNoEndSeparator(archivePath, true);
            String archivePathWithFileName = archivePath + "/" + folderName;

            File sourceDir1 = new File(archivePathWithFileName);
            if (!sourceDir1.exists()) {
                sourceDir1.mkdir();
            }
            File sourceDir = new File(sourcePath);
            File[] listFiles = sourceDir.listFiles();
            for (File file : listFiles) {
                if (file.isDirectory()) {
                    FileUtils.copyDirectoryToDirectory(file, sourceDir1);
                } else {
                    FileUtils.copyFileToDirectory(file, sourceDir1);
                }
            }
            try (FileOutputStream fos = new FileOutputStream(archivePathWithFileName + ".zip");
                    ZipOutputStream zipOut = new ZipOutputStream(fos)) {
                File fileToZip = new File(archivePathWithFileName);

                zipFile(fileToZip, fileToZip.getName(), zipOut);

                FileUtils.cleanDirectory(fileToZip);
                FileUtils.deleteDirectory(fileToZip);
            }
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--zipTheSourceFolder(-,-)");
            SSISController.logger.error(e.getMessage());
        }
    }

    /**
     * this method creates a zip file for a given source directory files
     *
     * @param fileToZip
     * @param fileName
     * @param zipOut
     * @throws IOException
     */
    private static void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
        if (fileToZip.isHidden()) {
            return;
        }
        if (fileToZip.isDirectory()) {
            if (fileName.endsWith("/")) {
                zipOut.putNextEntry(new ZipEntry(fileName));
                zipOut.closeEntry();
            } else {
                zipOut.putNextEntry(new ZipEntry(fileName + "/"));
                zipOut.closeEntry();
            }
            File[] children = fileToZip.listFiles();
            for (File childFile : children) {
                zipFile(childFile, fileName + "/" + childFile.getName(), zipOut);
            }
            return;
        }
        try (FileInputStream fis = new FileInputStream(fileToZip)) {
            ZipEntry zipEntry = new ZipEntry(fileName);
            zipOut.putNextEntry(zipEntry);
            byte[] bytes = new byte[1024];
            int length;
            while ((length = fis.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--zipFile(-,-)");
            SSISController.logger.error(e.getMessage());
        }
    }

}
