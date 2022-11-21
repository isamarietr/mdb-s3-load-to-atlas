
if [ -f "./env.sh" ]; then
  source ./env.sh
fi

AWS_S3_LOCATION_ROOT=`dirname $AWS_S3_LOCATION`
ATLAS_BASE_URL="https://cloud.mongodb.com/api/atlas/v1.0/groups/${ATLAS_PROJECT_ID}/clusters/${ATLAS_CLUSTER_NAME}"
MAPPINGS=`cat ../mappings.json`

INDEX_DEFINITION='{
    "collectionName": "'$TARGET_COLLECTION'",
    "database": "'$TARGET_DATABASE'",
    "mappings": '$MAPPINGS',
    "name": "default"
  }'

echo "Creating new search index on $TARGET_DATABASE.$TARGET_COLLECTION with index definition:"
echo  "$INDEX_DEFINITION"
curl -u $ATLAS_API_KEY --digest --location --request POST "$ATLAS_BASE_URL/fts/indexes" \
--header 'Content-Type: application/json'  --data-raw "$INDEX_DEFINITION"


start=$(date +%s)
aws s3 ls $AWS_S3_LOCATION --recursive | awk '{print $4}' | grep '\.gz' > temp.txt
while read -r line; do
    echo -e "\nImporting $AWS_S3_LOCATION_ROOT/$line"
    aws s3 cp $AWS_S3_LOCATION_ROOT/$line - | gzip -d - | mongoimport -d $TARGET_DATABASE -c $TARGET_COLLECTION $ATLAS_CONNECTION_STRING
done < temp.txt
rm temp.txt

end=$(date +%s)
duration=$(($end-$start))
echo "Elapsed Time: $duration seconds ($(($duration/60)) minutes)"