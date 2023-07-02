

data=f"{profile_file}#{share_name}.program.program"

# Create a SharingClient.
client = delta_sharing.load_as_spark(data)
print(client)

shared_df = delta_sharing.load_as_spark(table_url)
 
display(shared_df)