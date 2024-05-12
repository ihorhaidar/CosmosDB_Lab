from azure.cosmos import CosmosClient, PartitionKey

url = 'https://it-step-cosmos-db2.documents.azure.com:443/'
key = ''

client = CosmosClient(url, credential=key)

database_name = 'LabDatabase'
database = client.create_database_if_not_exists(id=database_name)

container_name = 'LabContainer'
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/category"),
    offer_throughput=400
)

print("Setup complete - Database and Container are ready")

for i in range(15):
    container.upsert_item({
        'id': str(i),
        'category': 'SampleCategory',
        'name': f'Item - {i}',
        'description': f'This is a sample description N {i}'
    })

print("Documents created.")

query = "SELECT * FROM c WHERE c.category='SampleCategory'"
items = list(container.query_items(
    query=query,
    enable_cross_partition_query=True
))

for item in items:
    print(item)

item_to_update = items[0]
item_to_update['description'] = 'Updated description.'
container.replace_item(item=item_to_update['id'], body=item_to_update)


