/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author: chuanchuan.lcc
 * @date: 2021-07-11 11:46
 * @modifiedBy: chuanchuan.lcc
 * @version: 1.0
 * @description:
 */
public class UUIDsTest {

    /**
     * fQGvk3oBw2H72wHxjjsQ
     */
    @Test
    public void base64UUID() {
        String aa = UUIDs.base64UUID();
        System.out.println(aa);

        aa = UUIDs.base64UUID();
        System.out.println(aa);
    }


    /**
     * 8h2fig4PTzSID0n9R8FiSw
     * ZNZHDP2SQdmYWYwbfQVMrA
     */
    @Test
    public void base64UUID1() {
        String aa = UUIDs.randomBase64UUID(new Random());
        System.out.println(aa);

        aa = UUIDs.randomBase64UUID(new Random());
        System.out.println(aa);
    }


    /**
     * AXqTtMIuevQBWIjppjC3
     * AXqTtMIvevQBWIjppjC4
     */
    @Test
    public void base64UUID2() {
        String aa = UUIDs.legacyBase64UUID();
        System.out.println(aa);

        aa = UUIDs.legacyBase64UUID();
        System.out.println(aa);
    }
}
