package tk.feelai.bigquery;

/*
 * Copyright (c) 2017 Hiroshi Matsuda.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class Mysqldump2csv {

    final Logger logger = LoggerFactory.getLogger(Mysqldump2csv.class);

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("java tk.feelai.bigquery.Mysqldump2csv outDir [-z] [dumpSql1 [dumpSql2 [...]]]");
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return;
        }
        File outDir = new File(args[0]);
        Mysqldump2csv mysqldump2Csv = new Mysqldump2csv(outDir);
        int index = 1;
        boolean useZip;
        if (args.length > index && "-z".equalsIgnoreCase(args[index])) {
            useZip = true;
            index++;
        } else {
            useZip = false;
        }
        BufferedReader in;
        if (args.length == index) {
            in = new BufferedReader(new InputStreamReader(System.in));
            try {
                mysqldump2Csv.conv(in, useZip);
            } finally {
                in.close();
            }
        } else {
            for (int a = index; a < args.length; a++) {
                String fileName = args[a];
                if (fileName.endsWith(".zip")) {
                    in = new BufferedReader(new InputStreamReader(new ZipInputStream(new FileInputStream(fileName))));
                } else {
                    in = new BufferedReader(new FileReader(fileName));
                }
                try {
                    mysqldump2Csv.conv(in, useZip);
                } finally {
                    in.close();
                }
            }
        }
    }
    
    public Mysqldump2csv(File outDir) {
        this.outDir = outDir;
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
    }

    final private File outDir; 
    public long maxCsvLength = 0x8000000;
    public Pattern createTable = Pattern.compile("^CREATE TABLE `(.+)` \\($");
    public Pattern insertInto = Pattern.compile("^INSERT INTO `(.+)` VALUES (.+)$");
    public Pattern allowPattern = Pattern.compile(".+");
    public Pattern skipPattern = Pattern.compile("^$");

    public boolean skip(String tableName) {
        return !allowPattern.matcher(tableName).matches() || skipPattern.matcher(tableName).matches();
    }

    public void conv(BufferedReader in, boolean useZip) throws IOException {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                Matcher createTableMatcher = createTable.matcher(line);
                if (createTableMatcher.matches()) {
                    saveSchema(createTableMatcher.group(1), in);
                    continue;
                }
                Matcher insertIntoMatcher = insertInto.matcher(line);
                if (insertIntoMatcher.matches()) {
                    saveRecords(insertIntoMatcher.group(1), insertIntoMatcher.group(2), useZip);
                    continue;
                }
            }
        } finally {
            if (outRecords != null) {
                outRecords.close();
                outRecords = null;
            }
        }
    }

    public Pattern field = Pattern.compile("^  `(.+)` ([^,A-Z]+)( .+|,)$");
    public Pattern end = Pattern.compile("^\\).+;$");
    // BOOLEANはスキーマとフィールドの突き合わせが必要なのであえてtinyintをintegerに含めている
    public Pattern integerPattern = Pattern.compile("^(int|bigint|tinyint).+$");
    public Pattern floatPattern = Pattern.compile("^(float|double).+$");
    public Pattern timestampPattern = Pattern.compile("^(date|time).+$");
    public static final String SCHEMA_FILE_NAME_FORMAT = "%s.schema";

    public void saveSchema(String tableName, BufferedReader in) throws IOException {
        PrintWriter out;
        if (skip(tableName)) {
            out = DummyWriter.create();
            logger.debug("skipping schema: {}", tableName);
        } else {
            out = new PrintWriter(new File(outDir, String.format(SCHEMA_FILE_NAME_FORMAT, tableName)));
            logger.debug("retrieving schema: {}", tableName);
        }
        try {
            String line;
            while ((line = in.readLine()) != null) {
                if (end.matcher(line).matches()) {
                    break;
                }
                Matcher f = field.matcher(line);
                if (!f.matches()) {
                    continue;
                }
                String title = f.group(1);
                String type;
                if (integerPattern.matcher(f.group(2)).matches()) {
                    type = "INTEGER";
                } else if (floatPattern.matcher(f.group(2)).matches()) {
                    type = "FLOAT";
                } else if (timestampPattern.matcher(f.group(2)).matches()) {
                    type = "TIMESTAMP";
                } else {
                    type = "STRING";
                }
                out.println(String.format("%s\t%s", title, type));
            }
        } finally {
            out.close();
        }
    }

    public void saveRecords(String tableName, String line, boolean useZip) throws IOException {
        PrintWriter out = getRecordWriter(tableName, line.length(), useZip);
        boolean inRecord = false, inQuote = false, inEscape = false, beginning = false, firstRecrod = true;
        int pos = 0, fieldMax = 1, fieldCount = 1;
        while (pos < line.length()) {
            char c = line.charAt(pos++);
            if (inEscape) {
                inEscape = false;
                continue;
            }
            switch (c) {
            case '(':
                if (inQuote) {
                    out.print(c);
                    beginning = false;
                } else if (inRecord) {
                    throw new IllegalStateException("nested '(': " + line);
                } else {
                    inRecord = true;
                    beginning = true;
                    fieldCount = 1;
                }
                break;
            case ')':
                if (inQuote) {
                    out.print(c);
                } else if (!inRecord) {
                    throw new IllegalStateException("not corresponding ')': " + line);
                } else {
                    out.println();
                    inRecord = false;
                    firstRecrod = false;
                }
                beginning = false;
                break;
            case '\'':
                if (!inRecord) {
                    throw new IllegalStateException("illegal quote: " + line);
                }
                inQuote = !inQuote;
                out.print('"');
                beginning = false;
                break;
            case '"':
                if (!inRecord) {
                    throw new IllegalStateException("illegal double quote: " + line);
                }
                out.print(' ');
                beginning = false;
                break;
            case '\\':
                if (!inQuote) {
                    throw new IllegalStateException("illegal escape char: " + line);
                }
                inEscape = true;
                out.print(' ');
                beginning = false;
                break;
            case ',':
                if (inQuote) {
                    out.print(c);
                    beginning = false;
                } else if (inRecord) {
                    if (firstRecrod) {
                        fieldMax++;
                    } else if (++fieldCount > fieldMax) {
                        throw new IllegalStateException("field count exceeds " + fieldMax + "\n" + line);
                    }
                    out.print(c);
                    beginning = true;
                } else if (!inRecord) {
                    beginning = false;
                }
                break;
            case ';':
                if (inQuote) {
                    out.print(c);
                } else if (inRecord) {
                    throw new IllegalStateException("illegal line termination (in record): " + line);
                } else if (pos != line.length()) {
                    throw new IllegalStateException("illegal line termination (line continued): " + line);
                } else {
                    return;
                }
                beginning = false;
                break;
            case 'N': // NULLは出力しない
                if (beginning) {
                    pos += 3;
                } else {
                    out.print(c);
                }
                beginning = false;
                break;
            default:
                out.print(c);
                beginning = false;
                break;
            }
        }
        throw new IllegalStateException("illegal line termination (without ';'): " + line);
    }

    public static final String CSV_FILE_NAME_PATTERN = "^%s\\.([0-9]+)\\.csv(\\.zip)?$";
    public static final String CSV_FILE_NAME_FORMAT = "%s.%03d.csv%s";
    private String prevTableName;
    private PrintWriter outRecords;
    private long outLength;
    private int outIndex;
    
    public PrintWriter getRecordWriter(String tableName, int lineLength, boolean useZip) throws IOException {
        if (outRecords == null) {
        } else if (!tableName.equals(prevTableName)) {
            outRecords.close();
            outRecords = null;
            outLength = 0;
            outIndex = 0;
        } else if(outLength > 0 && outLength + lineLength > maxCsvLength) {
            outRecords.close();
            outRecords = null;
            outLength = 0;
            outIndex++;
        } else {
            outLength += lineLength;
            return outRecords;
        }
        if (skip(tableName)) {
            outRecords = DummyWriter.create();
            if (!tableName.equals(prevTableName)) {
                prevTableName = tableName;
                logger.debug("skipping records: {}", tableName);
            }
        } else {
            if (!tableName.equals(prevTableName)) {
                prevTableName = tableName;
                logger.debug("retrieving records: {}");
            }
            File file = new File(outDir, String.format(CSV_FILE_NAME_FORMAT, tableName, outIndex, useZip ? ".zip" : ""));
            if (useZip) {
                outRecords = new PrintWriter(new OutputStreamWriter(new ZipOutputStream(new FileOutputStream(file))));
            } else {
                outRecords = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)));
            }
            outLength = lineLength;
        }
        return outRecords;
    }
    
    public static boolean firstCsvExists(File dir, String tableName) {
        Pattern p = Pattern.compile(String.format(CSV_FILE_NAME_PATTERN, tableName));
        for (String file : dir.list()) {
            Matcher m = p.matcher(file);
            if (m.matches() && Integer.parseInt("0" + m.group(1)) == 0) {
                return true;
            }
        }
        return false;
    }
    
    private static class DummyWriter extends PrintWriter {
        public static DummyWriter create() throws IOException {
            File dummy = File.createTempFile("dummy", "");
            dummy.deleteOnExit();
            return new DummyWriter(dummy);
        }
        private DummyWriter(File temp) throws IOException {
            super(temp);
        }
        @Override
        public void println() {}
        @Override
        public void println(String s) {}
        @Override
        public void print(String s) {}
        @Override
        public void print(char c) {}
        @Override
        public void print(int c) {}
    }
}
