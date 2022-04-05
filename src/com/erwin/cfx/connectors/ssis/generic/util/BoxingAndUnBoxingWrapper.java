/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 17-08-2021
 */
public class BoxingAndUnBoxingWrapper {

    BoxingAndUnBoxingWrapper() {
    }

    /**
     * this method will return int number as output back to the method calling
     * providing String based number is mandatory
     *
     * @param stringNumber
     * @return number
     */
    public static int convertStringToInteger(String stringNumber) {
        int conversionNumber = 0;
        try {
            stringNumber = stringNumber.replace("\"", "");
            conversionNumber = Integer.parseInt(stringNumber);

        } catch (Exception e) {
            conversionNumber = 0;
        }
        return conversionNumber;
    }

    /**
     * this method returns string value for a given int value converting int to
     * string
     *
     * @param number
     * @return
     */
    public static String convertIntegerToString(int number) {
        String conversionString = "";
        try {
            conversionString = number + "";
        } catch (Exception e) {
            conversionString = "";
        }
        return conversionString;
    }

    /**
     * this method will return float number as output back to the method calling
     * providing String based number is mandatory
     *
     * @param stringNumber
     * @return number
     */
    public static float convertStringToFloat(String stringNumber) {
        float conversionNumber = 0;
        try {
            conversionNumber = Float.parseFloat(stringNumber);

        } catch (Exception e) {
            conversionNumber = 0;
        }
        return conversionNumber;
    }

    /**
     * this method will return Boolean as output back to the method calling
     * providing String based Boolean is mandatory
     *
     * @param stringBoolean
     * @return Boolean
     */
    public static boolean convertStringToBoolean(String stringBoolean) {

        boolean flag;
        try {
            flag = Boolean.parseBoolean(stringBoolean);

        } catch (Exception e) {
            flag = false;
        }
        return flag;
    }

}
