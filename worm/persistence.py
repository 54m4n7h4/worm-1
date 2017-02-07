
class QueryException(Exception):
    pass


class MultipleObjectsReturned(QueryException):
    pass


class ObjectNotFound(QueryException):
    pass


class GenericObject(object):
    pass


class Manager(object):
    def __init__(self, database, mapping=None, model=GenericObject):
        self._database = database
        self._mapping = mapping
        self.model = model

    @property
    def database(self):
        return self._database

    @property
    def mapping(self):
        return self._mapping

    def begin(self):
        self._database.start_transaction()

    def commit(self):
        self._database.commit_transaction()

    def rollback(self):
        self._database.rollback_transaction()

    def add(self, obj):
        self._database.insert(self._mapping, obj)

    def add_many(self, objs):
        self._database.insert_many(self._mapping, objs)

    def update(self, obj):
        self._database.update(self._mapping, obj)

    def update_many(self, objs):
        self._database.update_many(self._mapping, objs)

    def delete(self, obj):
        self._database.delete(self._mapping, obj)

    def delete_many(self, obj):
        self._database.delete_many(self._mapping, obj)

    def all(self):
        return self._database.select_all(self._mapping, self.model)

    def query(self, sql, query_args=None):
        return self._database.raw(sql, self._mapping, self.model, query_args)

    def clone(self, database=None, mapping=None, model=None):
        database = database or self._database
        mapping = mapping or self._mapping
        model = model or self.model
        return type(self)(database=database, mapping=mapping, model=model)

    # shortcuts

    def values(self, *args, **kw):
        return self.all().values(*args, **kw)

    def values_list(self, *args, **kw):
        return self.all().values_list(*args, **kw)

    def values_flat(self, *args, **kw):
        return self.all().values_flat(*args, **kw)

    def count(self, *args, **kw):
        return self.all().count(*args, **kw)


class RelationManager(Manager):
    def __init__(self, query, query_args, ctx, *args, **kw):
        self._rel_query = query
        self._rel_ctx = ctx
        self._rel_query_args = query_args
        super(RelationManager, self).__init__(*args, **kw)

    def all(self):
        return self.query(self._rel_query, self._rel_query_args)

    def query(self, sql, query_args=None):
        return self._database.raw(
                sql, self._mapping, self.model, query_args, self._rel_ctx)
