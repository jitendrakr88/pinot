/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools;

import java.util.concurrent.Callable;


/**
 * Interface class for pinot-admin commands.
 *
 *
 */
public interface Command extends Callable<Integer> {

  default Integer call() throws Exception {
    // run execute() and returns 0 if success otherwise return -1.
    return execute() ? 0 : -1;
  }

  boolean execute()
      throws Exception;

  void printUsage();

  String description();
}
