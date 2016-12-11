Make a query request
====================

new endpoint : /api/purge (POST) 
 example: 
   ```
     curl   -H "Content-Type: application/json"   -X PUT   -d '{"family":"dog_registry2","date":"12/12/2016 18:33:55"}'   http://localhost:8080/api/purge
   ```
Simply provide the family thay you would like purge and the cutoff date and time and you will be able to purge data dynamically 

There's a global deletion feature that purge all of the data after 7 days

