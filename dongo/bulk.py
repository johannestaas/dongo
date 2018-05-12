'''
dongo
=====

A Django-ORM inspired Mongo ODM.
'''
from pymongo import DeleteMany, UpdateOne, ReplaceOne, DeleteOne

from .exceptions import DongoResultError

__all__ = ('DongoBulk', 'DongoLazyUpdater')


def _inst_to_query(query):
    if '_id' in query:
        return {'_id': query['_id']}
    elif '_uuid' in query:
        return {'_uuid': query['_uuid']}
    else:
        raise DongoResultError(
            'cant set in instance that was never inserted and assigned an _id '
            'yet'
        )


class DongoLazyUpdater(object):
    '''
    A wrapper around a DongoBulk and DongoCollection instance to allow lazy
    updates.
    '''

    def __init__(self, instance):
        '''
        Create a lazy updater wrapper over an instance of a DongoCollection::

            person = Person.new(name='joe')
            lazy = DongoLazyUpdater(person)
            lazy['foo'] = 'bar'
            lazy.save()

        The easier interface is to just invoke ``lazy()`` from the instance::

            lazy = person.lazy()

        You can add lazy updaters together and perform a save all at once::

            lazy1 = person1.lazy()
            lazy2 = person2.lazy()
            bulk = lazy1 + lazy2
            bulk.save()

        :param instance: the instance to wrap
        :return: a new lazy updater
        '''
        self.instance = instance
        self.ops = []

    def __add__(self, other):
        if isinstance(other, DongoBulk):
            other.ops.extend(self.ops)
            self.ops = []
            return other
        elif isinstance(other, DongoLazyUpdater):
            bulk = DongoBulk()
            bulk.ops.extend(self.ops + other.ops)
            self.ops = []
            other.ops = []
            return bulk
        else:
            raise TypeError('cant add DongoLazyUpdater to anything other than '
                            'another lazy updater or a DongoBulk instance')

    def __getitem__(self, attr):
        return self.instance[attr]

    def __setitem__(self, attr, val):
        query = _inst_to_query(self.instance)
        upd = UpdateOne(query, {'$set': {attr: val}}, upsert=False)
        self.instance._set_nested(attr, val)
        self.ops.append(upd)

    def set(self, **kwargs):
        '''
        Assign some deferred updates to be performed on ``save()``

        :param **kwargs: the fields to update with their value
        :return: the result of ``pymongo.UpdateOne``
        '''
        query = _inst_to_query(self.instance)
        dct = self.instance._translate_dunder_dict(kwargs)
        for key, val in dct.items():
            self.instance._set_nested(key, val)
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

    def __add__(self, other):
        if isinstance(other, (DongoLazyUpdater, DongoBulk)):
            self.ops.extend(other.ops)
            other.ops = []
            return self
        else:
            raise TypeError('cant add DongoBulk to anything other than a '
                            'lazy updater or another DongoBulk instance')

    def take(self, lazy):
        '''
        Take the bulk operations that a DongoLazyUpdater or other DongoBulk was
        going to perform to aggregate into one DongoBulk::

            person1 = Person.new(name='joe')
            person2 = Person.new(name='jill')
            lazy1 = person1.lazy()
            lazy2 = person2.lazy()
            lazy1.set(name='joejoe', age=30)
            lazy2.set(age=50)
            lazy1['foo'] = 'bar'

            bulk = Person.bulk()
            bulk.take(lazy1)
            bulk.take(lazy2)

            bulk2 = Person.bulk()
            bulk2.update_one(person1, name='hello')
            bulk.take(bulk2)

            bulk.save()

            lazy1['favorite_number'] = 12
            # this only performs the one ``favorite_number`` update
            lazy1.save()

        The shorthand for take is simply addition assignment::

            bulk += lazy1 + bulk2 + lazy2 + bulk3
            bulk.save()

        You can simply add DongoLazyUpdaters together as well::

            (lazy1 + lazy2).save()

        :param lazy: the ``DongoLazyUpdater`` or other ``DongoBulk``
        '''
        self.ops.extend(lazy.ops)
        lazy.ops = []

    def update_one(self, instance, **kwargs):
        '''
        Update a single instance with a deferred update operation::

            person1 = Person.new(name='joe', age=30)
            person2 = Person.new(name='jill', age=40)
            bulk = Person.bulk()
            bulk.update_one(person, name='joejoe')
            bulk.update_one(person2, name='jilljill', age=41)
            bulk.save()
            Person.refresh_all_from_db([person1, person2])
            # joe is named joejoe, and jill is named jilljill and 41 years old

        :param instance: the instance to add an update op to
        :return: the ``pymongo.UpdateOne`` result
        '''
        query = _inst_to_query(instance)
        upd = UpdateOne(query, {'$set': kwargs}, upsert=False)
        self.ops.append(upd)
        return upd

    def inc_one(self, instance, **kwargs):
        '''
        Increment a single instance with an amount::

            person1 = Person.new(name='joe', age=30)
            person2 = Person.new(name='jill', age=40)
            bulk = Person.bulk()
            bulk.inc_one(person, age=1)
            bulk.inc_one(person2, age=1)
            bulk.save()
            Person.refresh_all_from_db([person1, person2])
            # Now joe and jill are 1 year older

        :param instance: the instance to add an increment op to
        :return: the ``pymongo.UpdateOne`` result
        '''
        query = _inst_to_query(instance)
        upd = UpdateOne(query, {'$inc': kwargs}, upsert=False)
        self.ops.append(upd)
        return upd

    def replace_one(self, instance, **kwargs):
        '''
        Replace a single instance's document entirely::

            person1 = Person.new(name='joe', age=30)
            person2 = Person.new(name='jill', age=40)
            bulk = Person.bulk()
            bulk.replace_one(person, name='joejoe', age=50)
            bulk.replace_one(person2, name='jilly', age=60)
            bulk.save()
            Person.refresh_all_from_db([person1, person2])
            # Now their documents are overwritten entirely

        :param instance: the instance to add a replace op to
        :return: the ``pymongo.ReplaceOne`` result
        '''
        query = _inst_to_query(instance)
        rep = ReplaceOne(query, kwargs, upsert=False)
        self.ops.append(rep)
        return rep

    def delete_one(self, instance):
        '''
        Delete a document.

            person1 = Person.new(name='joe', age=30)
            person2 = Person.new(name='jill', age=40)
            bulk = Person.bulk()
            bulk.update_one(person, name='joejoe', age=50)
            bulk.delete_one(person2)
            bulk.save()
            Person.refresh_all_from_db([person1])
            # Now jill is gone

        :param instance: the instance to add a delete op to
        :return: the ``pymongo.DeleteOne`` result
        '''
        query = _inst_to_query(instance)
        delete = DeleteOne(query)
        self.ops.append(delete)
        return delete

    def delete_many(self, **kwargs):
        '''
        Delete many documents.

            person1 = Person.new(name='joe', age=30)
            person2 = Person.new(name='jill', age=31)
            person3 = Person.new(name='bob', age=50)
            bulk = Person.bulk()
            bulk.delete_many(age__gt=30)
            bulk.save()
            # Now jill and bob are gone, having age > 30

        :param **kwargs: the query to run the delete many with
        :return: the ``pymongo.DeleteMany`` result
        '''
        query, _ = self.klass._build_query(kwargs)
        delete = DeleteMany(query)
        self.ops.append(delete)
        return delete

    def save(self):
        '''
        Perform all the saved bulk operations.

        :return: the result from the ``pymongo.bulk_write``
        '''
        result = None
        if self.ops:
            result = self.klass.coll.bulk_write(self.ops)
        self.ops = []
        return result
