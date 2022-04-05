/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 16-08-2021
 */
public class Constants {

    Constants() {

    }

    public static String commonRegularExpression = "[^a-zA-Z0-9 \\p{L}_-]";
    public static String dtsx = "dtsx";
    public static String expressionLiteral = "DTS:Expression";
    public static String objectNameLiteral = "DTS:ObjectName";
    public static String executableLiteral = "DTS:Executable";
    public static String external = ":external";
    public static String invalid = ":invalid";
    public static String connections = "connections";
    public static String NOT_DEFINED = "NOT_DEFINED";
    public static String cachedName = "cachedName";
    public static String cachedLength = "cachedLength";
    public static String dataType = "dataType";
    public static String length = "length";
    public static String queryConstant = "que_ry12";
    public static String tableConstant = "tableNa_me34";

    public static String nameLiteral = "DTS:Name";
    public static String propertyLiteral = "DTS:Property";
    public static String objectDataLiteral = "DTS:ObjectData";
    public static String sqlTaskDataLiteral = "SQLTask:SqlTaskData";
    public static String sqlStatementLiteral = "SQLTask:SqlStatementSource";
    public static String sqlConnectionLiteral = "SQLTask:Connection";
    public static String textLiteral = "#text";
    public static String objectLiteral = "ObjectName";
    public static String dtsIdLiteral = "DTSID";

}
