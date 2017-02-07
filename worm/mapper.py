
class MappingError(Exception):
    pass


class ColumnAlreadyMapped(MappingError):
    pass


class PropertyAlreadyMapped(MappingError):
    pass


class Relation(object):
    def __init__(self, column, mapping, query=None, model=None, many=True):
        self.column = column
        self.mapping = mapping
        self.model = model
        self.query = query
        self.many = many


class Mapping(object):
    def __init__(
            self, table=None, mapping=None, primary_key=None, workspace=None):
        self._c2p = {}
        self._p2c = {}
        self._dc = []
        self._rels = {}
        self._tablename = table
        self._pk = primary_key or []
        self._workspace = workspace

        if mapping:
            for columnname, propname in mapping.items():
                self.add(columnname, propname)

    def add(self, column_name, property_name):
        if column_name in self._c2p:
            raise ColumnAlreadyMapped(column_name)

        if property_name in self._p2c:
            raise PropertyAlreadyMapped(property_name)

        self._c2p[column_name] = property_name
        self._p2c[property_name] = column_name

        if column_name not in self._pk:
            self._dc.append(column_name)

    def add_relation(
            self, prop, via, mapping, query=None, model=None, many=True):

        property_name = prop
        rel_column = via

        if property_name in self._rels:
            raise PropertyAlreadyMapped(property_name)

        if not model and self._workspace:
            _models = self._workspace.mapping_models(mapping)
            if len(_models) == 1:
                model = _models[0]
            else:
                raise ValueError(
                    "Can't automatically match model to the mapping. "
                    "You must set `model` argument.")

        self._rels[property_name] = Relation(
                rel_column, mapping, query=query, model=model, many=many)

        return self._rels[property_name]

    def row_to_object(self, row, obj):
        for colname, propname in self._c2p.items():
            try:
                value = row[colname]
            except (IndexError, KeyError):
                value = None
            setattr(obj, propname, value)
        return obj

    def object_to_dict(self, obj):
        result = {}
        for propname, colname in self._p2c.items():
            try:
                result[colname] = getattr(obj, propname)
            except AttributeError:
                pass  # skip, won't be updated
        return result

    def columns(self):
        return self._c2p.keys()

    def data_columns(self):
        return self._dc[:]

    def primary_key(self):
        return self._pk[:]

    def simple_pk(self):
        return self._pk[0]

    def properties(self):
        return self._p2c.keys()

    def relations(self):
        return self._rels.items()

    @property
    def db_table(self):
        return self._tablename
