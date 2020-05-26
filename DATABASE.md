Here are described some useful mongo manipulations.

## Manually manage indices

All of the commands described in this section are done in the mongoshell.

- To inspect the indices of a collection:

```
db.Collection.getIndexes()
```

- To create an indices on a collection:

Here is a example with a compound index, a partialFilterExpression (with this, the index will only be applied to documents verifying the condition), a unique constraint.

```
db.Collection.createIndex(
    {
        "identifier.system": 1,
        "identifier.value": 1,
        "identifier.type.coding.0.system": 1,
        "identifier.type.coding.0.code": 1
    }, 
    {
        partialFilterExpression: {identifier: {$exists: true}}
    },
    unique=true
)
```

- To remove an index on a collection:

```
db.Collection.dropIndex(indexName)
```


## mongodump and mongorestore

All of the commands described in this section are **outside** the mongoshell.

These commands can be useful to do manual backups before an operation on the mongo DB.

They can also be used for adding unique indices on a collection: mongorestore will try to insert the documents one by one and if one insertion raises a DuplicateKeyError, it will be skipped.

### mongodump

```
mongodump --host=<host> --port=<port> --username=<username> --password=<password> --authenticationDatabase=admin --db=<database-name> --collection=<collection-name>
```

host and port are optional if you are working with mongo default host and port.

db and collection are optional if you want to dump everything.

This command will write files to a `dump/` folder by default. You can specify the target folder with the flag `--out`.

### mongorestore

```
mongodump --host=<host> --port=<port> --username=<username> --password=<password> --authenticationDatabase=admin --nsInclude=<path.to.restore> dump/
```

host and port are optional if you are working with mongo default host and port.

nsInclude is optional if you want to restore all the dump. Otherwise you use it by adding the path to restore (for instance "databaseName.collectionName"). You use this flag several times.

`dump/` at the end is the default folder where `mongodump` writes, you can use another folder there as well.