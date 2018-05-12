'''
dongo
=====

A Django-ORM inspired Mongo ODM.
'''
from pymongo import DeleteMany, UpdateOne, ReplaceOne, DeleteOne

from .exceptions import DongoResultError

__all__ = ('DongoBulk', 'DongoLazyUpdater')


def _query_or_inst(query):
    if '_id' in query:
        return {'_id': query['_id']}
    elif '_uuid' in query:
        return {'_uuid': query['_uuid']}
    elif hasattr(query, '_data'):
        return query._data
    else:
        return query


class DongoLazyUpdater(object):
    '''
    A wrapper around a DongoBulk and DongoCollection instance to allow lazy
    updates.
    '''

    def __init__(self, instance):
        '''
        Create a lazy updater wrapper over an instance of a DongoCollection::

            >> person = Person.new(name='joe')
            >> lazy = DongoLazyUpdater(person)
            >> lazy['foo'] = 'bar'
            >> lazy.save()

        The easier interface is to just invoke ``lazy()`` from the instance::

            >> lazy = person.lazy()

        :param instance: the instance to wrap
        :return: a new lazy updater
        '''
        self.instance = instance
        self.ops = []

    def __getitem__(self, attr):
        return self.instance[attr]

    def __setitem__(self, attr, val):
        query = self._safe_query_or_inst()
        upd = UpdateOne(query, {'$set': {attr: val}}, upsert=False)
        self.instance._set_nested(attr, val)
        self.ops.append(upd)

    def _safe_query_or_inst(self):
        query = _query_or_inst(self.instance)
        if not ('_id' in query or '_uuid' in query):
            raise DongoResultError(
                'cant set in instance that was never inserted and assigned '
                'an _id yet'
            )
        return query

    def set(self, **kwargs):
        '''
        Assign some deferred updates to be performed on ``save()``

        :param **kwargs: the fields to update with their value
        :return: the result of ``pymongo.UpdateOne``
        '''
        query = self._safe_query_or_inst()
        dct = self.instance._translate_dunder_dict(kwargs)
        for key, val in dct.items():
            self._set_nested(key, val)
        upd = UpdateOne(query, {'$set': kwargs}, upsert=False)
        self.ops.append(upd)
        return upd

    def save(self):
        '''
        Perform the deferred updates in a bulk operation.

        :return: the result of ``pymongo.bulk_write``
        '''
        result = None
        if self.ops:
            result = self.instance.__class__.coll.bulk_write(self.ops)
        self.ops = []
        return result


class DongoBulk(object):
    '''
    A collection of bulk operations to be performed.
    '''

    def __init__(self, klass, ops=None):

        self.klass = klass
        self.ops = ops or []

    def update_one(self, query, _upsert=False, **kwargs):
        query = _query_or_inst(query)
        upd = UpdateOne(query, {'$set': kwargs}, upsert=_upsert)
        self.ops.append(upd)
        return upd

    def inc_one(self, query, _upsert=False, **kwargs):
        query = _query_or_inst(query)
        upd = UpdateOne(query, {'$inc': kwargs}, upsert=_upsert)
        self.ops.append(upd)
        return upd

    def replace_one(self, query, _upsert=False, **kwargs):
        query = _query_or_inst(query)
        rep = ReplaceOne(query, kwargs, upsert=_upsert)
        self.ops.append(rep)
        return rep

    def delete_one(self, *args, **kwargs):
        if args:
            query = _query_or_inst(query)
        else:
            query = kwargs
        delete = DeleteOne(query)
        self.ops.append(delete)
        return delete

    def delete_many(self, **kwargs):
        delete = DeleteMany(kwargs)
        self.ops.append(delete)
        return delete

    def save(self):
        result = None
        if self.ops:
            result = self.klass.coll.bulk_write(self.ops)
        self.ops = []
        return result
