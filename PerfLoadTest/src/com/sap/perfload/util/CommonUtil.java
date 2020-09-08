package com.sap.perfload.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

public class CommonUtil {

	public final static String commonLetters = "etaoinsrhldcumfp";

	public static Properties loadProperties(String propFileName) throws Exception {
		Properties props = new Properties();
		InputStream input = new FileInputStream(propFileName);
		props.load(input);

		return props;
	}

	public static String generateUniqueKey() {
		StringBuffer key = new StringBuffer();
		key.append(Calendar.getInstance().getTimeInMillis());
		for (int i = 0; i < 10; i++) {
			key.insert((int) (Math.random() * key.length()), (char) (90 - Math.random() * 25));
		}
		return key.toString();
	}

	/**
	 * 
	 * @param length
	 * @return
	 */
	public static String generateRandomString(String letters, int length, String separator) {
		if (letters == null || letters.trim().equals("")) {
			letters = commonLetters;
		}
		if (length > commonLetters.length()) {
			return commonLetters;
		}
		StringBuilder strBuilder = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			// generate a random number between 0 to length
			int index = (int) (commonLetters.length() * Math.random());

			// add Character one by one in end of sb
			strBuilder.append(commonLetters.charAt(index));
			if (separator != null) {
				strBuilder.append(separator);
			}
		}

		return strBuilder.toString();
	}

	/**
	 * 
	 * @param fileName
	 * @param content
	 */
	public static void writeLineToFile(String fileName, String content) {
		FileWriter fw = null;
		try {
			File f = new File(fileName);
			fw = new FileWriter(f, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintWriter pw = new PrintWriter(fw);

		pw.println(content);
		pw.flush();

		try {
			fw.flush();
			pw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param folder
	 * @param fileNamePattern
	 */
	public static void removeFiles(String folder, String fileNamePattern) {
		File currentDir = new File(folder);
		for (File f : currentDir.listFiles()) {
			if (f.getName().startsWith(fileNamePattern)) {
				f.delete();
			}
		}
	}

	public static void removeFile(String filePath) {
		File file = new File(filePath);
		if (file.exists()) {
			file.delete();
		}
	}

	/**
	 * 
	 * @param fileName
	 * @return
	 */
	public static List<String> readFileLinesToList(String fileName) {
		List<String> lines = new ArrayList<String>();
		try {

			File file = new File(fileName);

			BufferedReader br = new BufferedReader(new FileReader(file));
			String readLine = "";

			while ((readLine = br.readLine()) != null) {
				lines.add(readLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}
}
