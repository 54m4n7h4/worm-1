import types

from .persistence import Manager
from .mapper import Mapping


class Workspace(object):
    def __init__(self, database=None):
        self._mappings = {}  # model_class -> mapping
        self.database = database

    def register_mapping(self, mapping, model_class):
        self._mappings[model_class] = mapping

    def create_mapping(
            self, model_class, table, mapping=None, columns=None,
            primary_key=None):
        """
        Creates and registers mapping for `model_class` to table `table`
        using `mapping` (column->property) dict or `columns` list.

        Set `primary_key` columns list to define primary key.
        """

        if columns and mapping:
            raise TypeError('Please use `mapping` OR `columns` arguments')

        if not columns and not mapping:
            raise TypeError(
                    'One of `columns` or `mapping` argument is required')

        if columns:
            mapping = dict(zip(columns, columns))

        if isinstance(primary_key, types.StringTypes):
            primary_key = [primary_key]

        mapping_obj = Mapping(
                table, mapping=mapping, primary_key=primary_key,
                workspace=self)

        self.register_mapping(mapping_obj, model_class)

        return mapping_obj

    def model_mapping(self, model_class):
        return self._mappings[model_class]

    def mapping_models(self, mapping):
        return map(
                lambda x: x[0],
                filter(lambda x: mapping is x[1], self._mappings.items()))

    def manage(self, model, database=None):
        return Manager(
                database=database or self.database,
                mapping=self.model_mapping(model),
                model=model)


workspace = Workspace()  # default workspace
