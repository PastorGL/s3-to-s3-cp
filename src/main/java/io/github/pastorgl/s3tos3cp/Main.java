package io.github.pastorgl.s3tos3cp;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    private static final Pattern PATTERN = Pattern.compile("^s3://([^/]+)/(.+)");
    private static final Options OPTIONS = new Options();

    private static int _15MB = 15 * 1024 * 1024;

    static {
        OPTIONS.addRequiredOption("k", "prefixFrom", true, "s3 key prefix to copy objects from");
        OPTIONS.addRequiredOption("K", "prefixTo", true, "s3 key prefix to copy objects to");
        OPTIONS.addOption("a", "accessFrom", true, "s3 access key to copy objects from");
        OPTIONS.addOption("A", "accessTo", true, "s3 access key to copy objects to");
        OPTIONS.addOption("s", "secretFrom", true, "s3 secret key to copy objects from");
        OPTIONS.addOption("S", "secretTo", true, "s3 secret key to copy objects to");
        OPTIONS.addOption("v", "verbose", false, "show some stats");
    }

    public static void main(String[] args) throws Exception {
        CommandLine cmd = new DefaultParser().parse(OPTIONS, args);

        String from = cmd.getOptionValue("k");
        String to = cmd.getOptionValue("K");


        Matcher m = PATTERN.matcher(from);
        if (!m.matches()) {
            helpAndExit(5);
        }

        String bucketFrom = m.group(1);
        String keyPrefixFrom = m.group(2);


        AmazonS3ClientBuilder fromBuilder = AmazonS3ClientBuilder.standard()
                .enableForceGlobalBucketAccess();
        String accessFrom = cmd.getOptionValue("a", null);
        if (accessFrom != null) {
            String secretFrom = cmd.getOptionValue("s", null);
            if (secretFrom == null) {
                helpAndExit(4);
            }
            fromBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessFrom, secretFrom)));
        }
        AmazonS3 s3from = fromBuilder.build();

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketFrom);
        request.setPrefix(keyPrefixFrom);


        m = PATTERN.matcher(to);
        if (!m.matches()) {
            helpAndExit(6);
        }
        String bucketTo = m.group(1);
        String keyPrefixTo = m.group(2);


        AmazonS3ClientBuilder toBuilder = AmazonS3ClientBuilder.standard()
                .enableForceGlobalBucketAccess();
        String accessTo = cmd.getOptionValue("A", null);
        if (accessTo != null) {
            String secretTo = cmd.getOptionValue("S", null);
            if (secretTo == null) {
                helpAndExit(3);
            }
            toBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessTo, secretTo)));
        }
        AmazonS3 s3to = toBuilder.build();


        ObjectListing objectListing = s3from.listObjects(request);
        List<S3ObjectSummary> objectSummaries = new ArrayList<>(objectListing.getObjectSummaries());
        while (objectListing.isTruncated()) {
            objectListing = s3from.listNextBatchOfObjects(objectListing);
            objectSummaries.addAll(objectListing.getObjectSummaries());
        }

        boolean verbose = cmd.hasOption("v");
        long ts0 = new Date().getTime();
        long total = 0L;
        if (verbose) {
            total = objectSummaries.stream().map(S3ObjectSummary::getSize).reduce(Long::sum).get();
            System.out.println(from + " -> " + to + ": " + objectSummaries.size() + " object(s), " + total + " byte(s)");
        }

        objectSummaries.stream()
                .map(S3ObjectSummary::getKey).parallel()
                .forEach(keyFrom -> {
                    try {
                        S3Object s3object = s3from.getObject(bucketFrom, keyFrom);
                        long size = s3object.getObjectMetadata().getContentLength();
                        String keyTo = keyPrefixTo + "/" + keyFrom;

                        if (verbose) {
                            System.out.println(keyFrom + " -> " + keyTo + ": " + size + " byte(s)");
                        }

                        if (size > 0L) {
                            S3ObjectInputStream in = s3object.getObjectContent();
                            StreamTransferManager stm = new StreamTransferManager(bucketTo, keyTo, s3to);

                            MultiPartOutputStream out = stm.numStreams(1)
                                    .numUploadThreads(1)
                                    .queueCapacity(1)
                                    .partSize(15)
                                    .getMultiPartOutputStreams().get(0);

                            byte[] buf = new byte[_15MB];
                            int read;
                            while (true) {
                                read = in.read(buf);
                                if (read > 0) {
                                    out.write(buf, 0, read);
                                } else {
                                    break;
                                }
                            }

                            in.close();
                            out.close();
                            stm.complete();
                        } else {
                            s3to.putObject(bucketTo, keyTo, "");
                        }
                    } catch (IOException e) {
                        System.err.println(e.getMessage());
                    }
                });

        if (verbose) {
            double time = (new Date().getTime() - ts0) / 1000.D;
            System.out.println(time + " second(s), ~" + (total / 1024 / 1024 / time) + " MB/sec");
        }
    }

    private static void helpAndExit(int code) {
        new HelpFormatter().printHelp("s3 to s3 copy utility", OPTIONS);
        System.exit(-code);
    }
}
