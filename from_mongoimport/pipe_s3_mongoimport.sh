
source ./env.sh

ATLAS_BASE_URL="https://cloud.mongodb.com/api/atlas/v1.0/groups/${ATLAS_PROJECT_ID}/clusters/${ATLAS_CLUSTER_NAME}"
MAPPINGS=`cat ../mappings.json`
echo "$MAPPINGS"
exit 0

# echo "Creating new collection $TARGET_DATABASE.$TARGET_COLLECTION"
# curl --verbose -u $ATLAS_API_KEY --digest --location --request POST "$ATLAS_BASE_URL/index" \
# --header 'Content-Type: application/json' \
# --data-raw '{
#     "db": "from_pipe_db",
#     "collection": "from_pipe_coll",
#     "keys": [
#         {
#             "id": 1
#         }
#     ]
# }'

echo "Creating new search index on $TARGET_DATABASE.$TARGET_COLLECTION"
curl -u $ATLAS_API_KEY --digest --location --request POST "$ATLAS_BASE_URL/fts/indexes" \
--header 'Content-Type: application/json' \
--data-raw '{
    "collectionName": "'$TARGET_COLLECTION'",
    "database": "'$TARGET_DATABASE'",
    "mappings": { "dynamic": true },
    "name": "default"
  }'


start=$(date +%s)
aws s3 ls $AWS_S3_LOCATION --recursive | awk '{print $4}' | grep '\.gz' > temp.txt
while read -r line; do
    echo "Piping $AWS_S3_LOCATION_ROOT/$line"
    aws s3 cp $AWS_S3_LOCATION_ROOT/$line - | gzip -d - | mongoimport -d $TARGET_DATABASE -c $TARGET_COLLECTION $ATLAS_CONNECTION_STRING
done < temp.txt

rm temp.txt

end=$(date +%s)
duration=$(($end-$start))
echo "Elapsed Time: $duration seconds ($(($duration/60)) minutes)"