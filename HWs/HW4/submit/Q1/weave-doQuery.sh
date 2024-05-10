echo '{
  "query": "{
    Get{
      MySimSearch (
        limit: 5
        nearText: {
          concepts: [\"anime movies with love story\"],
        }
      ){
        name
        genre
        type
        rating
      }
    }
  }"
}'  | curl \
    -X POST \
    -H 'Content-Type: application/json' \
    -d @- \
    localhost:8080/v1/graphql