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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobList;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.BigqueryScopes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;

public class CsvUploader {

    final Logger logger = LoggerFactory.getLogger(CsvUploader.class);

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("java tk.feelai.bigquery.CsvUploader project_id dataset_id service_account_email client_secret_p12_file_path dump_dir [(Mysqldump2csv_options | -stdin)]");
        System.err.println("  dump_dir must contain results of Mysqldump2csv.");
        System.err.println("  dump_dir will be over-written by Dump2scv results if Mysqldump2csv_options or -stdin is specified.");
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            usage();
            return;
        }
        String target = args[4];
        if (args.length >= 6) {
            if ("-stdin".equalsIgnoreCase(args[5])) {
                Mysqldump2csv.main(new String[] { target });
            } else {
                String[] dumpArgs = new String[args.length - 4];
                System.arraycopy(args, 4, dumpArgs, 0, dumpArgs.length);
                Mysqldump2csv.main(dumpArgs);
            }
        }
        final Logger logger = LoggerFactory.getLogger(CsvUploader.class); // for static context should not be shared
        CsvUploader uploader = new CsvUploader();
        logger.debug("authorizing ...");
        uploader.authorize(args[0], args[1], args[2], new File(args[3]));
        logger.debug(" done");
        uploader.prepareDataset();
        uploader.uploadAll(new File(target), true, false, 0);
    }
    /**
     * Retry until retryMax times when exception thrown while executing callable.call().
     * The retry interval is 10 seconds.
     */
    private static <T> T autoRetry(int retryMax, Callable<T> callable) throws Exception {
        final Logger logger = LoggerFactory.getLogger(CsvUploader.class); // for static context should not be shared
        Exception lastException = null;
        for (int a = 0; a <= retryMax; a++) {
            try {
                return callable.call();
            } catch (Exception e) {
                lastException = e;
                logger.error(e.toString());
            }
            if (a < retryMax) {
                try {
                    logger.warn("auto-retry in 10 seconds");
                    Thread.sleep(10000);
                } catch (Exception e) {
                }
            }
        }
        throw new IllegalStateException(lastException);
    }
    /**
     * schemaFile format is "field_name\tfield_type\n"
     */
    private static TableSchema loadSchema(File schemaFile) throws IOException {
        TableSchema schema = new TableSchema();
        ArrayList<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        BufferedReader in = new BufferedReader(new FileReader(schemaFile));
        try {
            String line;
            while ((line = in.readLine()) != null) {
                String[] f = line.split("\t");
                TableFieldSchema s = new TableFieldSchema();
                s.setName(f[0]);
                s.setType(f[1]);
                fields.add(s);
            }
            schema.setFields(fields);
        } finally {
            in.close();
        }
        return schema;
    }

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("^([^.]+)\\..+$"); 

    private HttpTransport httpTransport;
    private String projectId;
    private String datasetId;
    private Credential credential;
    private Bigquery bigquery;
    private Dataset dataset;
    
    public CsvUploader() throws Exception {
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    }
    
    /**
     * Authorizes the installed application to access user's protected data.
     */
    public void authorize(String projectId, String datasetId, String accountId, File clientSecret) throws Exception {
        credential = new GoogleCredential.Builder().setTransport(httpTransport)
            .setJsonFactory(JSON_FACTORY)
            .setServiceAccountId(accountId)
            .setServiceAccountScopes(Collections.singleton(BigqueryScopes.BIGQUERY))
            .setServiceAccountPrivateKeyFromP12File(clientSecret)
            .build();
        this.projectId = projectId;
        this.datasetId = datasetId;
    }
    
    /**
     * Prepare Bigquery Dataset object.
     */
    public void prepareDataset() throws Exception {
        logger.debug("connecting to {}", projectId);
        bigquery = new Bigquery.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName("tk.feelai.bigquery.CsvUploader").build();
        logger.debug(" done");

        logger.debug("getting dataset list ...");
        DatasetList datasetList = bigquery.datasets().list(projectId).execute();
        logger.debug(" done");
        try {
            for (Datasets d : datasetList.getDatasets()) {
                if (String.format("%s:%s", projectId, datasetId).equals(d.getId())) {
                    dataset = autoRetry(6, new Callable<Dataset>() {
                        @Override
                        public Dataset call() throws Exception {
                            return bigquery.datasets().get(projectId, datasetId).execute();
                        }
                    });
                    break;
                }
            }
        } catch (NullPointerException e) {
        }
        if (dataset == null) {
            logger.debug("creating {}", datasetId);
            DatasetReference datasetRef = new DatasetReference()
                .setProjectId(projectId)
                .setDatasetId(datasetId);
            final Dataset outputDataset = new Dataset()
                .setDatasetReference(datasetRef);
            dataset = autoRetry(6, new Callable<Dataset>() {
                @Override
                public Dataset call() throws Exception {
                    return bigquery.datasets().insert(projectId, outputDataset).execute();        
                }
            });
            logger.debug(" done");
        } else {
            logger.debug("attached to {}", datasetId);
        }
        JobList jobList = bigquery.jobs().list(projectId).execute();
        logger.debug("recent job list is below:");
        logger.debug(jobList.getJobs().toString());
    }

    /**
     * Upload all the tables contained in specified directory.
     * @param dir   Upload target directory which contains *.schema and *.csv files.
     * @param directUploadEnabled The false value recommended. You can use true for this argument whenever the sizes of each file are less than 1MB but there are very few merit.
     * @param useGZipContent    The false value recommended. If true, the upload speed will slow down heavily.
     * @param maxBadRecords The 0 value is recommended to detect all errors.
     */
    public void uploadAll(final File dir, boolean directUploadEnabled, boolean useGZipContent, int maxBadRecords) throws Exception {
        logger.debug("upload target directory is {}. options: directUploadEnabled={}, useGZipContent={}, maxBadRecords={}",
                dir,
                directUploadEnabled,
                useGZipContent,
                maxBadRecords);
        Queue<File> queue = new LinkedList<File>();
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".schema");
            }
        });
        Arrays.sort(files);
        for (final File file : files) {
            queue.add(file);
        }
        while (queue.size() > 0) {
            final File schema = queue.poll();
            final String fileName = schema.getName();
            Matcher matcher = FILE_NAME_PATTERN.matcher(fileName);
            if (!matcher.matches()) {
                throw new IllegalStateException();
            }
            final String tableName = matcher.group(1);
            try {
                uploadTable(tableName, schema, directUploadEnabled, useGZipContent, maxBadRecords);
            } catch (Exception e) {
                queue.add(schema);
                logger.error("Exception occured and appended to retry queue", e);
            }
        }
        logger.debug("upload completed in " + dir);
    }

    /**
     * Read the schema csv and records csv files.
     * If the ID field values are all 0 or missing ID field, the bigquery table will be recreated.
     * In other cases, new records are appended to the existing bigquery table.
     * @param tableName This argument value is used as "${tableName}.([0-9]+.)?.csv" to read the records of the table.
     * @param schema    The schema file of the table.
     * @param directUploadEnabled The false value recommended. You can use true for this argument whenever the sizes of each file are less than 1MB but there are very few merit.
     * @param useGZipContent    The false value recommended. If true, the upload speed will slow down heavily.
     * @param maxBadRecords The 0 value is recommended to detect all errors.
     */
    public void uploadTable(final String tableName, final File schema, boolean directUploadEnabled, boolean useGZipContent, int maxBadRecords) throws Exception {
        logger.debug("getting table list ...");
        TableList tableList = bigquery.tables().list(projectId, datasetId).execute();
        logger.debug(" done");

        File dir = schema.getParentFile();
        boolean renew = Mysqldump2csv.firstCsvExists(dir, tableName), exists = false;
        try {
            for (Tables t : tableList.getTables()) {
                if (String.format("%s:%s.%s", projectId, datasetId, tableName).equals(t.getId())) {
                    exists = true;
                    if (renew) {
                        logger.debug("deleting " + tableName + " table ...");
                        bigquery.tables().delete(projectId, datasetId, tableName).execute();
                        logger.debug(" done");
                    }
                    break;
                }
            }
        } catch (NullPointerException e) {
            logger.debug("  NullPointerException ignored");
        }
        
        final TableReference tref = insertTable(tableName, loadSchema(schema), renew || !exists);
        uploadCsvIntoTable(dir, tableName, tref, directUploadEnabled, useGZipContent, maxBadRecords);
    }

    /**
     * Return a TableReference on bigquery repository.
     * If create is true, the table will be inserted to the bigquery repository.
     */
    public TableReference insertTable(String tableName, TableSchema schema, boolean create) throws Exception {
        TableReference tref = new TableReference()
            .setProjectId(projectId)
            .setDatasetId(datasetId)
            .setTableId(tableName);
        if (!create) {
            return tref;
        }
        logger.debug("inserting {} table ...", tableName);
        final Table table = new Table()
            .setSchema(schema)
            .setTableReference(tref);
        autoRetry(6, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                bigquery.tables().insert(projectId, datasetId, table).execute();
                return null;
            }
        });
        logger.debug(" done");
        return tref;
    }

    /**
     * Upload all the csv records contained in specified directory.
     * @param dir   Upload target directory which contains *.schema and *.csv files.
     * @param tableName The table name which is used for the part of Mysqldump2csv.CSV_FILE_NAME_PATTERN.
     * @param tref  The TableReference for the bigquery table to insert.
     * @param directUploadEnabled The false value recommended. You can use true for this argument whenever the sizes of each file are less than 1MB but there are very few merit.
     * @param useGZipContent    The false value recommended. If true, the upload speed will slow down heavily.
     * @param maxBadRecords The 0 value is recommended to detect all errors.
     * @throws Exception    An IllegalStateException will be thrown when the retry exceeds 10 times.
     */
    public void uploadCsvIntoTable(File dir, final String tableName, TableReference tref, final boolean directUploadEnabled, final boolean useGZipContent, final int maxBadRecords) throws Exception {
        logger.debug("  upload records into {}", tableName);
        JobConfigurationLoad jobLoad = new JobConfigurationLoad()
            .setDestinationTable(tref)
            .setCreateDisposition("CREATE_NEVER")
            .setWriteDisposition("WRITE_APPEND")
            .setMaxBadRecords(maxBadRecords)
            .setSourceUris(null);
        JobConfiguration jobConfig = new JobConfiguration()
            .setLoad(jobLoad);
        JobReference jobRef = new JobReference()
            .setProjectId(projectId);
        final Job outputJob = new Job()
            .setConfiguration(jobConfig)
            .setJobReference(jobRef);

        final Pattern fileNamePattern = Pattern.compile(String.format(Mysqldump2csv.CSV_FILE_NAME_PATTERN, tableName));
        File[] csvs = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return fileNamePattern.matcher(name).matches();
            }
        });
        if (csvs.length == 0) {
            logger.warn("  no record found in {}", tableName);
        }
        Arrays.sort(csvs);
        long totalSize = 0;
        for (File csv : csvs) {
            totalSize += csv.length();
        }
        logger.debug(String.format("  total %d files, %,3dkB", csvs.length, totalSize / 1000));

        final UploadAdaptor ua = new UploadAdaptor(totalSize);
        for (final File csv : csvs) {
            autoRetry(10, new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    logger.debug("  target={}.", csv);
                    Matcher matcher = fileNamePattern.matcher(csv.getName());
                    if (!matcher.matches()) {
                        throw new IllegalStateException();
                    }
                    String zip = matcher.group(2);
                    InputStream in;
                    if (zip == null || zip.length() == 0) {
                        in = new BufferedInputStream(new FileInputStream(csv));
                    } else {
                        in = new ZipInputStream(new FileInputStream(csv));
                    }
                    try {
                        InputStreamContent mediaContent = new InputStreamContent("application/octet-stream", in);
                        // GCPライブラリの制限でdirectUploadEnabledが有効な場合にコンテントの長さをセットするとgzipが無効になる。
                        if (directUploadEnabled || !useGZipContent) {
                            mediaContent.setLength(csv.length());
                        }
                        Insert insert = bigquery.jobs().insert(projectId, outputJob, mediaContent);
                        insert.getMediaHttpUploader()
                            .setDirectUploadEnabled(directUploadEnabled)
                            .setDisableGZipContent(!useGZipContent)
                            .setProgressListener(ua);
                        JobStatus status = insert.execute().getStatus();
                        if (status.getErrors() != null && status.getErrors().size() > 0) {
                            
                            throw new IllegalStateException("job has error(s) " + status.getErrors());
                        }
                        ua.uploadedSize += csv.length();
                        return null;
                    } finally {
                        in.close();
                    }
                }
            });
        }
    }
    
    private class UploadAdaptor implements MediaHttpUploaderProgressListener {
        final long totalSize;
        long uploadedSize, prev, prevSize;
        UploadAdaptor(long totalSize) {
            this.totalSize = totalSize;
        }
        public void progressChanged(MediaHttpUploader uploader) throws IOException {
            switch (uploader.getUploadState()) {
            case INITIATION_STARTED:
                logger.debug("INITIATION_STARTED");
                prev = System.currentTimeMillis();
                prevSize = 0;
                break;
            case INITIATION_COMPLETE:
                logger.debug("INITIATION_COMPLETE");
                break;
            case MEDIA_IN_PROGRESS:
                long now = System.currentTimeMillis();
                if (now - prev >= 60000) {
                    long size = uploader.getNumBytesUploaded();
                    logger.debug("MEDIA_IN_PROGRESS");
                    logger.debug(String.format("    %,3dkB, %.2f%% (%,3dkB/sec), " + new Date(now) + " .", (uploadedSize + size) / 1000, (uploadedSize + size) * 100f / totalSize, (size - prevSize) / (now - prev)));
                    prev = now;
                    prevSize = size;
                }
                break;
            case MEDIA_COMPLETE:
                logger.debug("MEDIA_COMPLETE");
                break;
            case NOT_STARTED:
                throw new IllegalStateException("upload not started");
            }
        }};
}
