#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
module Shell
  module Commands
    class WalArchive < Command
      def help
        <<-EOF
Archive the earliest log files. If too many regions need to be flushed,
it will cost a few time.
A 'server_name' is the host, port plus startcode of a regionserver.
For example: host187.example.com,60020,1289493121758 (find servername in
master ui or when you do detailed status in shell)
EOF
      end

      def command(server_name)
        admin.wal_archive(server_name)
      end
    end
  end
end
