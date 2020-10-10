/*
 *   Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.neptune.gremlin.driver.example;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;

/**
 * An example client code to demonstrate the process of making auth enabled Gremlin calls to Neptune Server.
 * If auth is enabled on the server side then the neptune db region should be set either as a system property of as
 * an environment variable.
 * For instance, set the region as system property: <code>-DSERVICE_REGION=us-east-1</code>
 * <p>
 * The request signing logic requires IAM credentials to sign the requests. Two of the methods to provide the IAM
 * credentials:
 * <ol>
 *     <li>Setting environment variables AWS_ACCESS_KEY_ID=[your-access-key-id] and AWS_SECRET_KEY=[your-access-secret].</li>
 *     <li>Passing as JVM arg: -Daws.accessKeyId=[your-access-key-id] and -Daws.secretKey=[your-access-secret].</li>
 * </ol>
 *
 * @see <a href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html">
 *     DefaultAWSCredentialsProviderChain"</a> for more information and additional methods for providing IAM credentials.
 */
public final class NeptuneGremlinSigV4Example {
    /**
     * Command line option name for the db cluster/instance endpoint.
     */
    private static final String ENDPOINT = "endpoint";
    /**
     * Command line option name for the db cluster/instance port.
     */
    private static final String PORT = "port";
    /**
     * Command line option name for the whether to use ssl connection.
     */
    private static final String SSL = "ssl";

    /**
     * The gremlin query to test.
     */
    private static final String SAMPLE_QUERY = "g.V().count()";

    /**
     * Default private constructor.
     */
    private NeptuneGremlinSigV4Example() {

    }

    /**
     * Test code to make gremlin java calls.
     * @param args program args.
     */
    public static void main(final String[] args) {

        final Options options = setupCliOptions();
        final CommandLine cli = parseArgs(args, options);
        final Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(cli.getOptionValue(ENDPOINT));
        builder.port(Integer.parseInt(cli.getOptionValue(PORT)));

        //If the neptune db is auth enabled then add use the following channelizer. Otherwise omit the below line.
        builder.channelizer(SigV4WebSocketChannelizer.class);
        builder.enableSsl(Boolean.parseBoolean(cli.getOptionValue(SSL, "false")));


        final Cluster cluster = builder.create();
        try {
            final Client client = cluster.connect();
            final ResultSet rs = client.submit(SAMPLE_QUERY);

            for (Result r : rs) {
                System.out.println(r);
            }
        } finally {
            cluster.close();
        }
    }

    /**
     * Parses the command line args and returns a {@link CommandLine} with the properties.
     * @param args the command line args.
     * @param options the options object containing the args that can be passed.
     * @return a {@link CommandLine} instance with the option properties set.
     */
    private static CommandLine parseArgs(final String[] args, final Options options) {
        final CommandLineParser parser = new BasicParser();
        final HelpFormatter formatter = new HelpFormatter();

        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            formatter.printHelp(NeptuneGremlinSigV4Example.class.getSimpleName(), options);
            throw new RuntimeException("Invalid command line args");
        }
    }

    /**
     * Private utility to set the CLI options required to run the program.
     * @return {@link Options} that can be accepted by the program.
     */
    private static Options setupCliOptions() {
        final Options options = new Options();

        final Option endpoint = new Option("e", ENDPOINT, true, "The db cluster/instance endpoint");
        endpoint.setRequired(true);
        options.addOption(endpoint);

        final Option port = new Option("p", PORT, true, "The db cluster/instance port");
        port.setRequired(true);
        options.addOption(port);

        final Option ssl = new Option("s", SSL, true, "Whether to enable ssl on the connection");
        ssl.setRequired(false);
        ssl.setType(Boolean.class);
        options.addOption(ssl);

        return options;
    }
}
