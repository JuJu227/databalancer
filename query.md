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
   {"result":["{spot,labrador,100}","{spot,labrador,100}"]}
   ```


