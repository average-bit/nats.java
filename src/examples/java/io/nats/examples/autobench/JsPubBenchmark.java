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
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsPubBenchmark extends AutoBenchmark {
    private final boolean file;
    private final boolean sync;
    private final boolean saveForSub;
    private final List<String> subjects;

    public JsPubBenchmark(String name, long messageCount, long messageSize, boolean file, boolean sync, boolean saveForSub, List<String> subjects) {
        super(name, messageCount, messageSize);
        this.file = file;
        this.sync = sync;
        this.saveForSub = saveForSub;
        this.subjects = subjects;
    }

    public void execute(Options connectOptions) throws InterruptedException {
        byte[] payload = createPayload();
        String stream = JsPullSubBenchmark.getStream(getMessageCount(), getMessageSize());

        try {
            Connection nc = Nats.connect(connectOptions);

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subjects)
                    .storageType(file ? StorageType.File : StorageType.Memory)
                    .build();
            JetStreamManagement jsm = nc.jetStreamManagement();
            try {
                jsm.deleteStream(stream);
            }
            catch (Exception ignore) {}
            jsm.addStream(sc);

            JetStream js = nc.jetStream();

            try {
                this.startTiming();
                if (sync) {
                    for (int i = 0; i < this.getMessageCount(); i++) {
                        js.publish(subjects.get(i % subjects.size()), payload);
                    }
                }
                else {
                    for (int i = 0; i < this.getMessageCount(); i++) {
                        js.publishAsync(subjects.get(i % subjects.size()), payload);
                        if (i > 0 && i % 1000 == 0) {
                            Thread.sleep(1);
                        }
                    }
                }
                defaultFlush(nc);
                this.endTiming();
            } finally {
                try {
                    if (!saveForSub) {
                        jsm.deleteStream(stream);
                    }
                } catch (IOException | JetStreamApiException ex) {
                    this.setException(ex);
                }
                finally {
                    nc.close();
                }
            }
        } catch (IOException | JetStreamApiException ex) {
            this.setException(ex);
        }
    }
}
