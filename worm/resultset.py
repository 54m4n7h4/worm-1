

class ResultSet(object):
    def __init__(self, source, mapping, row_factory):
        self._source = source
        self._mapping = mapping
        self._row_factory = row_factory
        self._result = None

    def _fill_result(self):
        self._result = map(self._row_factory, self._source)

    @property
    def result(self):
        if self._result is None:
            self._fill_result()
        return self._result

    def __iter__(self):
        return iter(self.result)

    def __getitem__(self, idx):
        return self.result[idx]

    def __len__(self):
        return len(self.result)

    def count(self):
        return len(self)

    def values(self, *properties):
        res = []

        for obj in self.result:
            if not properties:
                mapping = getattr(obj, '__mapping', None) or self._mapping
                obj_props = mapping.properties()
                res.append(
                        dict(map(lambda y: (y, getattr(obj, y)), obj_props)))
        return res

    def values_list(self, *properties):
        if not properties:
            properties = self._mapping.properties()

        return map(
                lambda x: map(lambda y: getattr(x, y), properties),
                self.result)

    def values_flat(self, attrname):
        return map(lambda x: getattr(x, attrname), self.result)
