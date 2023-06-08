
if [ -f "./env.sh" ]; then
  source ./env.sh
fi

AWS_S3_LOCATION_ROOT=`dirname $AWS_S3_LOCATION`

start=$(date +%s)
aws s3 ls $AWS_S3_LOCATION --recursive | awk '{print $4}' > temp.txt
while read -r line; do
    echo -e "\nImporting $AWS_S3_LOCATION_ROOT/$line"
    aws s3 cp $AWS_S3_LOCATION_ROOT/$line - | mongoimport -d $TARGET_DATABASE -c $TARGET_COLLECTION $ATLAS_CONNECTION_STRING 
done < temp.txt
rm temp.txt

end=$(date +%s)
duration=$(($end-$start))
echo "Elapsed Time: $duration seconds ($(($duration/60)) minutes)"