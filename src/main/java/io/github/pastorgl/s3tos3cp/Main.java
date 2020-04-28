package io.github.pastorgl.s3tos3cp;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.io.IOException;
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
    }

    public static void main(String[] args) throws Exception {
        CommandLine cmd = new DefaultParser().parse(OPTIONS, args);

        String from = cmd.getOptionValue("k");
        String to = cmd.getOptionValue("K");


        Matcher m = PATTERN.matcher(from);
        if (!m.matches()) {
            System.exit(-5);
        }

        String bucketFrom = m.group(1);
        String keyPrefixFrom = m.group(2);


        AmazonS3ClientBuilder fromBuilder = AmazonS3ClientBuilder.standard()
                .enableForceGlobalBucketAccess();
        String accessFrom = cmd.getOptionValue("a", null);
        if (accessFrom != null) {
            String secretFrom = cmd.getOptionValue("s", null);
            if (secretFrom == null) {
                System.exit(-4);
            }
            fromBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessFrom, secretFrom)));
        }
        AmazonS3 s3from = fromBuilder.build();

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketFrom);
        request.setPrefix(keyPrefixFrom);


        m = PATTERN.matcher(to);
        if (!m.matches()) {
            System.exit(-6);
        }
        String bucketTo = m.group(1);
        String keyPrefixTo = m.group(2);


        AmazonS3ClientBuilder toBuilder = AmazonS3ClientBuilder.standard()
                .enableForceGlobalBucketAccess();
        String accessTo = cmd.getOptionValue("A", null);
        if (accessTo != null) {
            String secretTo = cmd.getOptionValue("S", null);
            if (secretTo == null) {
                System.exit(-3);
            }
            toBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessTo, secretTo)));
        }
        AmazonS3 s3to = toBuilder.build();


        s3from.listObjects(request).getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey).parallel()
                .forEach(k -> {
                    try {
                        S3Object s3object = s3from.getObject(bucketFrom, k);
                        long size = s3object.getObjectMetadata().getContentLength();

                        if (size > 0L) {
                            S3ObjectInputStream in = s3object.getObjectContent();

                            StreamTransferManager stm = new StreamTransferManager(bucketTo, keyPrefixTo + "/" + k, s3to);

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
                            s3to.putObject(bucketTo, keyPrefixTo + "/" + k, "");
                        }
                    } catch (IOException ignored) {
                    }
                });
    }
}
