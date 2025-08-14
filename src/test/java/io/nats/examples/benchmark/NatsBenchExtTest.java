package io.nats.examples.benchmark;

import io.nats.client.Nats;
import io.nats.client.Connection;
import io.nats.client.NatsTestServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NatsBenchExtTest {

    private NatsTestServer server;

    @BeforeEach
    public void setUp() throws Exception {
        server = new NatsTestServer(false);
    }

    @AfterEach
    public void tearDown() throws Exception {
        server.close();
    }

    @Test
    public void testPayloadSizes() throws Exception {
        String[] args = {
            "-s", server.getURI(),
            "--payload-sizes", "128,512,1024",
            "test-subject"
        };

        // Run the benchmark in a separate process
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-cp");
        command.add("build/libs/jnats-2.21.5-SNAPSHOT-fat.jar");
        command.add("io.nats.examples.benchmark.NatsBenchExt");
        for (String arg : args) {
            command.add(arg);
        }

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        // Read the output
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder output = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
        }
        process.waitFor();

        String result = output.toString();
        System.out.println(result);

        // Verify the output
        assertTrue(result.contains("Pub Only with payload size 128 stats:"));
        assertTrue(result.contains("Pub Only with payload size 512 stats:"));
        assertTrue(result.contains("Pub Only with payload size 1024 stats:"));
    }

    @Test
    public void testPullSingleFilter() throws Exception {
        // Restart server with JetStream enabled
        server.close();
        server = new NatsTestServer(false, true);

        String[] args = {
            "-s", server.getURI(),
            "-np", "1",
            "-ns", "1",
            "-n", "1000",
            "--pull-single-filter",
            "test-subject"
        };

        // Run the benchmark in a separate process
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-cp");
        command.add("build/libs/jnats-2.21.5-SNAPSHOT-fat.jar");
        command.add("io.nats.examples.benchmark.NatsBenchExt");
        for (String arg : args) {
            command.add(arg);
        }

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        // Read the output
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder output = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
        }
        process.waitFor();

        String result = output.toString();
        System.out.println(result);

        // Verify the output
        assertTrue(result.contains("Pull Consume Single Filter stats:"));
    }

    @Test
    public void testPullMultiFilter() throws Exception {
        // Restart server with JetStream enabled
        server.close();
        server = new NatsTestServer(false, true);

        String[] args = {
            "-s", server.getURI(),
            "-np", "1",
            "-ns", "1",
            "-n", "1000",
            "--pull-multi-filter",
            "--publish-subjects", "test-subject-1,test-subject-2",
            "test-subject-1" // Main subject, used for stream creation
        };

        // Run the benchmark in a separate process
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-cp");
        command.add("build/libs/jnats-2.21.5-SNAPSHOT-fat.jar");
        command.add("io.nats.examples.benchmark.NatsBenchExt");
        for (String arg : args) {
            command.add(arg);
        }

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        // Read the output
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder output = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
        }
        process.waitFor();

        String result = output.toString();
        System.out.println(result);

        // Verify the output
        assertTrue(result.contains("Pull Consume Multi Filter stats:"));
    }

    @Test
    public void testReqResWithPayloads() throws Exception {
        String[] args = {
            "-s", server.getURI(),
            "-np", "1",
            "-ns", "1",
            "-n", "1000",
            "--req-res",
            "--payload-sizes", "128,512",
            "test-subject"
        };

        // Run the benchmark in a separate process
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-cp");
        command.add("build/libs/jnats-2.21.5-SNAPSHOT-fat.jar");
        command.add("io.nats.examples.benchmark.NatsBenchExt");
        for (String arg : args) {
            command.add(arg);
        }

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        // Read the output
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder output = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
        }
        process.waitFor();

        String result = output.toString();
        System.out.println(result);

        // Verify the output
        assertTrue(result.contains("Req/Res with payload size 128 stats:"));
        assertTrue(result.contains("Req/Res with payload size 512 stats:"));
    }

    @Test
    public void testCorePub() throws Exception {
        String[] args = {
            "-s", server.getURI(),
            "-n", "1000",
            "--core-pub",
            "test-subject"
        };

        // Run the benchmark in a separate process
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-cp");
        command.add("build/libs/jnats-2.21.5-SNAPSHOT-fat.jar");
        command.add("io.nats.examples.benchmark.NatsBenchExt");
        for (String arg : args) {
            command.add(arg);
        }

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        // Read the output
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder output = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
        }
        process.waitFor();

        String result = output.toString();
        System.out.println(result);

        // Verify the output
        assertTrue(result.contains("Core Pub Only stats:"));
    }
}
