package com.dla.foundation.pio.itemSimilarity.test;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CSVFileHandler implements Closeable {

	private BufferedReader br = null;
	private final String separator;
	
	public CSVFileHandler(String csvpath, String separator) {
		try {
			br = new BufferedReader(new FileReader(csvpath));
			this.separator = separator;
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public String[] nextLine() throws IOException {
		String line = "";
			if ((line = br.readLine()) != null) {
				// use comma as separator
				String[] elements = line.trim().split(separator);
				return elements;
			}
		return null;
	}

	public void close() throws IOException {
		this.close();
		
	}

	/*public void close() throws IOException {
		this.close();
		
	}
*/
}
