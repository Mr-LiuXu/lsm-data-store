package com.lx.hashkvdb.utils;

public class DBUtils {

    /***
     * 构建路径名称
     * @param path
     * @param filename
     * @return
     */
    public static String buildFilename(String path, String filename) {
        StringBuilder sb = new StringBuilder(path);
        if (path.charAt(path.length() - 1) != '/') {
            sb.append('/');
        }
        sb.append(filename + ".log");
        return sb.toString();
    }

}
