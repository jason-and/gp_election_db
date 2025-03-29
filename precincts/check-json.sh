for file in *.geojson; do
  echo "File: $file"
  echo "Type: $(jq -r '.type' "$file")"
  echo "Property fields: $(jq -r '.features[0].properties' "$file")"
  echo "Feature count: $(jq -r '.features | length' "$file")"
  echo "-----------------------"
done
