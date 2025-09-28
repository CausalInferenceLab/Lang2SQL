docker run -d \
  --name pgvector \
  -e POSTGRES_PASSWORD=postgres \
  -p 5431:5432 \
  pgvector/pgvector:pg17  