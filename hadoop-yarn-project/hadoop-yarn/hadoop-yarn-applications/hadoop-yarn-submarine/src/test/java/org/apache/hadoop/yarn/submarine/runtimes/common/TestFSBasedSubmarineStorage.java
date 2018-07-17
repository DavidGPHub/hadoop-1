/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.runtimes.common;

import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.fs.MockRemoteDirectoryManager;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFSBasedSubmarineStorage {
  @Test
  public void testStorageOps() {
    MockRemoteDirectoryManager mrdm = new MockRemoteDirectoryManager();
    ClientContext clientContext = mock(ClientContext.class);
    when(clientContext.getRemoteDirectoryManager()).thenReturn(mrdm);
    FSBasedSubmarineStorageImpl storage = new FSBasedSubmarineStorageImpl(
        clientContext);
  }
}
