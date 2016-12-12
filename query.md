Make a query request
====================

new endpoint : /api/query (POST) 
 example: 
   ```
     curl -H "Content-Type: application/json" -X PUT -d '{"sql_query":"select * from dog_registry where name != \"spot\""}' http://localhost:8080/api/query
   ```

The clause_param isn't required, the api will simply return all content in that particular table

successful respond : 
   ```
      {"result":["[\"2\",\"max\",\"chihuahua\",\"3\",\"2016-12-11T11:45:06-05:00\"]","[\"3\",\"sprinkle\",\"pitbull\",\"50\",\"2016-12-11T11:45:06-05:00\"]"]}
   ```
   we're using real json objects 


