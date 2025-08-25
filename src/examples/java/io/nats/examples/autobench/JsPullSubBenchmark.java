// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.examples.autobench;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class JsPullSubBenchmark extends AutoBenchmark {

    private final List<String> subjects;
    private final boolean isMulti;

    public JsPullSubBenchmark(String name, long messageCount, long messageSize, boolean isMulti) {
        super(name, messageCount, messageSize);
        this.isMulti = isMulti;
        subjects = new ArrayList<>();
        if (isMulti) {
            subjects.add(getSubject(messageCount, messageSize, 1));
            subjects.add(getSubject(messageCount, messageSize, 2));
            subjects.add(getSubject(messageCount, messageSize, 3));
        }
        else {
            subjects.add(getSubject(messageCount, messageSize, 0));
        }
    }

    static String getStream(long messageCount, long messageSize) {
        return "strm-" + messageCount + "-" + messageSize;
    }

    static String getSubject(long messageCount, long messageSize, int index) {
        if (index == 0) {
            return "sub-" + messageCount + "-" + messageSize;
        }
        return "sub-" + messageCount + "-" + messageSize + "-" + index;
    }

    public void execute(Options connectOptions) throws InterruptedException {
        try {
            Connection nc = Nats.connect(connectOptions);
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            String streamName = getStream(getMessageCount(), getMessageSize());

            try {
                jsm.deleteStream(streamName);
            }
            catch (Exception ignore) {}

            StreamConfiguration sc = StreamConfiguration.builder()
                .name(streamName)
                .storageType(StorageType.Memory)
                .subjects(subjects)
                .build();
            jsm.addStream(sc);

            // This is a PULL subscription
            PullSubscribeOptions pso = PullSubscribeOptions.builder()
                .durable("dur-" + getMessageCount() + "-" + getMessageSize() + (isMulti ? "-multi" : ""))
                .build();

            String subscribeSubject;
            if (isMulti) {
                subscribeSubject = "sub-" + getMessageCount() + "-" + getMessageSize() + ".*";
            }
            else {
                subscribeSubject = subjects.get(0);
            }
            JetStreamSubscription sub = js.subscribe(subscribeSubject, pso);
            try {
                nc.flush(Duration.ofSeconds(1));
            }
            catch (Exception e) {
                this.setException(e);
                return;
            }

            long toReceive = getMessageCount();

            this.startTiming();
            long received = 0;
            while (received < toReceive) {
                List<Message> messages = sub.fetch(100, Duration.ofSeconds(1));
                received += messages.size();
                for (Message m : messages) {
                    m.ack();
                }
            }
            this.endTiming();

            try {
                jsm.deleteStream(streamName);
            }
            catch (Exception ignore) {}
            finally {
                nc.close();
            }
        } catch (IOException | JetStreamApiException ex) {
            this.setException(ex);
        }
    }
}
