/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.listener;

import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.options.CatalogOptions.ALLOWED_LISTENER;
import static org.apache.paimon.options.CatalogOptions.LISTENER_OPTIONS;

/**
 * aa.
 */
public class Listeners {

    private List<Listener> listeners = new ArrayList<>();

    private Listeners() {

    }

    public Listeners(List<Listener> listeners, Options options) {
        String[] names = StringUtils.split(options.get(ALLOWED_LISTENER), ",");
        if (names == null || names.length == 0) {
            return;
        }
        for (String name : names) {
            for (Listener listener : listeners) {
                if (listener.name().equals(name)) {
                    this.listeners.add(listener);
                }
            }
        }

        Map<String, String> properties = options.get(LISTENER_OPTIONS);
        Map<String, Map<String, String>> propertiesGroupByName = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String name = entry.getKey().split("\\.")[0];
            propertiesGroupByName.computeIfAbsent(name, k -> new HashMap<>())
                    .put(entry.getKey().replaceFirst(name + "\\.", ""), entry.getValue());
        }

        for (Listener listener : this.listeners) {
            listener.initialize(propertiesGroupByName.get(listener.name()));
        }
    }

    public void notify(Event event) {
        for (Listener listener : listeners) {
            listener.notify(event);
        }
    }

    public static Listeners emptyListeners() {
        return new Listeners();
    }
}
