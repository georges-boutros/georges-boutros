#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import delta_sharing

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "C:/Users/g-boutros/OneDrive - Arte Geie\DataLake - Azure - Power BI/github/datalakehouse-share/powerbi.share"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
print("########### All Available Tables #############")
#print(client.list_all_tables())
shares = client.list_shares()
 
for share in shares:
  schemas = client.list_schemas(share)
  for schema in schemas:
    tables = client.list_tables(schema)
    for table in tables:
      print(f'name = {table.name}, share = {table.share}, schema = {table.schema}')
      
# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#powerbi_share.program.program_active_stock"

# Fetch 10 rows from a table and convert it to a Pandas DataFrame. This can be used to read sample data from a table that cannot fit in the memory.
print("########### Loading 10 rows from powerbi_share.program.program_active_stock as a Pandas DataFrame #############")
data = delta_sharing.load_as_pandas(table_url, limit=10)

# Print the sample.
print("########### Show the fetched 10 rows #############")
print(data)

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
print("########### Loading from powerbi_share.program.program_active_stock as a Pandas DataFrame #############")
data = delta_sharing.load_as_pandas(table_url)

# Do whatever you want to your share data!
print("########### Show Data #############")
print(data[data["program"] == "055155-002-A"].head(10))


print("######### ############# ###########  Show  Date Share APIOS ########### ########### #############")   
# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "C:/Users/g-boutros/OneDrive - Arte Geie\DataLake - Azure - Power BI/github/datalakehouse-share/apios.share"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

shares = client.list_shares()
 
for share in shares:
  schemas = client.list_schemas(share)
  for schema in schemas:
    tables = client.list_tables(schema)
    for table in tables:
      print(f'name = {table.name}, share = {table.share}, schema = {table.schema}')
      
      # Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#apios_share.audience.audience_web_program_1y"

# Fetch 10 rows from a table and convert it to a Pandas DataFrame. This can be used to read sample data from a table that cannot fit in the memory.
#print("########### Loading 10 rows from apios_share.audience_web_program_1y as a Pandas DataFrame #############")
#data = delta_sharing.load_as_pandas(table_url, limit=10)

# Print the sample.
print("########### Show the fetched 10 rows #############")
print(data)

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
print("########### Loading from apios_share.audience.audience_web_program_1y as a Pandas DataFrame #############")
data = delta_sharing.load_as_pandas(table_url)

# Do whatever you want to your share data!
print("########### Show Data #############")
print(data[data["program"] == "055155-002-A"].head(10))