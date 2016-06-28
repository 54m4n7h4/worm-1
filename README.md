# worm
Damn simple DAO library (aka "Worst ORM ever" or "Wonderful ORM") FOR HUMANS ;)


## Quickstart example

This little snippet is an example of the idea.

The mapping is defined independly from any result class (called elsewhere a Model).
It allows to map relational data to instance of any class, and vice versa.
Only properties and column names matters in mapping process. 

DAO Manager is very limited. It can add, update and delete record, or do same on object lists.

Reading data is limited to two methods: `all()` and `query()`.
The `all()` shortcut selects all records from the datasource, as shown below:

```python

import worm
import worm.engines.psycopg2

usermapping = worm.Mapping(
    table='auth_user', mapping={
       'username': 'username',
       'email': 'email',
       },
    primary_key=['username'])
    
db = worm.database(worm.engines.psycopg2, dbname='test')

users = worm.Manager(db, usermapping)

print list(users.all())
```

For more complex filtering use a `query()` method, which takes "raw query" as an argument (typically plain SQL). 

```python
print list(users.query("select * from auth_user where username ilike 'a%' limit 10"))
```

