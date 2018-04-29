'''
dongo
==========

A Django-ORM inspired Mongo ODM.
'''

import re
from datetime import datetime
from hashlib import sha256
from uuid import uuid4, UUID

# third-party
import six
from pymongo import MongoClient, UpdateOne
from bson.objectid import ObjectId

# local
from .exceptions import (
    DongoConnectError,
    DongoResultError,
    DongoCollectionError,
)

__all__ = (
    'connect',
    'DongoClient',
    'DongoCollection',
    'QuerySet',
    'to_uuid',
)


RE_COMP_EXPR = re.compile(
    r'^(.*)(?:__(gte|lte|gt|lt|eq|ne|nin|in|regex|exists))$'
)
KEYWORD_TRANSLATIONS = {'no_timeout': 'no_cursor_timeout'}
CLIENT = None
DATABASE_NAME = None


class DongoClient(MongoClient):
    pass


def to_uuid(val):
    if isinstance(val, UUID):
        return val
    elif isinstance(val, int):
        return UUID(int=val)
    elif isinstance(val, six.string_types):
        if val.isdigit():
            return UUID(int=int(val))
        return UUID(val)
    raise ValueError('cant find uuid unless str, int or UUID type: {0!r}'
                     .format(val))


def connect(database, host='localhost', port=27017, uri=None, hosts=None,
            replica_set=None):
    '''
    Takes a default database, along with connection parameters.
    This also sets two global variables CLIENT and DATABASE_NAME.
    These are used for convenience for specifying a single connection which
    implicitly uses that client and database for all other defined classes.
    Otherwise, you can specify the database name in each class definition by
    adding the class variable ``database = 'my_database_name'``.
    To not use a default database name, specify ``connect(None)``

    All you need to get started is to specify connect before defining
    ``DongoCollection``'s::

        # connects to localhost:27017
        connect('mydatabase')

        # connects to a remote host
        connect('mydatabase', host='10.0.0.90')

        # connects to a replica set
        connect('mydatabase', hosts=['10.0.0.90', '10.0.0.91'],
                replica_set='a0')

    :param database: the default database name, required
    :param host: the host string for single host connection, default localhost
    :param hosts: multiple hosts for replica sets (list of host or host:port)
    :param replica_set: the name of the replica set
    :param port: the mongo port, default 27017
    :param uri: optionally specify uri explicitly
    :return: DongoClient instance
    '''
    global CLIENT, DATABASE_NAME
    DATABASE_NAME = database
    if uri:
        if replica_set:
            CLIENT = DongoClient(uri, replicaSet=replica_set)
        else:
            CLIENT = DongoClient(uri)
    elif hosts:
        # MongoReplicaSetClient is deprecated in latest pymongo
        if not replica_set:
            raise DongoConnectError('specify replicaset for multiple hosts')
        # build out correct hostname formats with ports
        port_hosts = []
        for hostname in hosts:
            # for explicitly setting the port of the host
            if ':' in hostname:
                port_hosts.append(hostname)
            else:
                port_hosts.append('{0}:{1}'.format(hostname, port))
        CLIENT = DongoClient(port_hosts, replicaSet=replica_set)
    else:
        uri = 'mongodb://{host}:{port}/'.format(host=host, port=port)
        CLIENT = DongoClient(uri)
    return CLIENT


class QuerySet(object):
    '''
    This is the result from calling a ``DongoCollection``'s ``filter``
    classmethod.

    You will only ever need to pull these by calling ``filter``::

        sel = MyDongoCollection.filter(some_field="value")
        for item in sel:
            print(item)

        sel.update(some_field="some new value")

    To fetch all records with x=5 and foo='bar'::

        MyClass.filter(x=5, foo='bar')

    To fetch all records where inside the "accounts" dictionary there's a
    "creditcard" property with value 1000::

        MyClass.filter(accounts__creditcard=1000)

    To fetch all records with x is greater or equal to 5::

        MyClass.filter(x__gte=5)

    To fetch all records where inside the "accounts" dictionary there's a
    "creditcard" property with value greater than 10000::

        MyClass.filter(accounts__creditcard__gt=10000)

    To fetch all records that have color property either red or blue::

        MyClass.filter(color__in=['red', 'blue'])

    To fetch all records where favorite color ISN'T blue::

        MyClass.filter(color__ne='blue')

    To fetch all records where favorite color isn't red or blue, where age
    is greater than 50, and where age is less than or equal to 60::

        MyClass.filter(color__nin=['red', 'blue'], age__gt=50, age__lte=60)
    '''

    def __init__(self, klass, query):
        self.klass = klass
        self.query, self.opts = klass._build_query(query)

    @classmethod
    def _filter_or(cls, klass):
        ms = cls(klass, {})
        ms.query = {'$or': []}
        return ms

    @classmethod
    def _filter_and(cls, klass):
        ms = cls(klass, {})
        ms.query = {'$and': []}
        return ms

    def __iter__(self):
        '''
        Iterate across all objects filtered.

        :yield: new class instance of record
        '''
        return self.iter(timeout=True)

    def _fix_sort_arg(self, kwargs):
        if 'sort' not in kwargs:
            return
        if isinstance(kwargs['sort'], six.string_types):
            kwargs['sort'] = [(kwargs['sort'], 1)]

    def __add__(self, other):
        if '$or' in self.query:
            op = '$or'
        elif '$and' in self.query:
            op = '$and'
        else:
            raise ValueError('top level must be $and or $or')
        self.query[op].append(other.query)
        return self

    def __or__(self, other):
        '''
        Add two query results together::

            for person in (
                PersonCollection.filter(age__gte=21) |
                PersonCollection.filter(birthday__lte=datetime(1997, 1, 1))
            ):
                print(person)

        :param other: the other filter
        :yield: each resulting class instance
        '''
        return QuerySet(self.klass, {'$or': [self.query, other.query]})

    def __and__(self, other):
        '''
        Find intersection between two query results::

            for person in (
                PersonCollection.filter(citizen='us') &
                PersonCollection.filter(age__gte=18)
            ):
                print('{} can vote'.format(person['name']))

        :param other: the other queryset
        :yield: each resulting class instance
        '''
        return QuerySet(self.klass, {'$and': [self.query, other.query]})

    def update(self, **updates):
        '''
        Run database updates to all filtered items::

            Person.filter(age=21).update(drinking=True)

        :param **updates: each keyword update and value
        :return: the pymongo update_many result
        '''
        return self.klass.coll.update_many(self.query, {'$set': updates})

    def inc(self, **updates):
        '''
        Run database increments to all filtered items::

            # Age each person by a year
            Person.filter().update(age=1)

        :param **updates: each field to increment and amount
        :return: the pymongo update_many result
        '''
        return self.klass.coll.update_many(self.query, {'$inc': updates})

    def delete(self, **kwargs):
        '''
        Delete all documents that match the queryset::

            # remove all accounts that haven't been active since 1/1/2000
            Account.filter(last_active__lte=datetime(2000, 1, 1).delete()

        :param **kwargs: keyword arguments to pass to pymongo's ``delete_many``
        :return: the pymongo delete_many result
        '''
        return self.klass.coll.delete_many(self.query, **kwargs)

    def first(self, **kwargs):
        '''
        Fetches the first matching filter or None.

        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param **kwargs: the keywords to pass to pymongo's ``find_one``
        :return: the class instance or None
        '''
        self._fix_sort_arg(kwargs)
        data = self.klass.coll.find_one(self.query, **kwargs)
        if not data:
            return None
        return self.klass(data)

    def first_or_die(self, **kwargs):
        '''
        Fetches the first matching filter or raises ``DongoResultError``.

        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param **kwargs: the keywords to pass to pymongo's ``find_one``
        :return: the class instance
        :raises: ``DongoResultError``
        '''
        data = self.first(**kwargs)
        if not data:
            raise DongoResultError('item not found with query')
        return data

    def iter(self, timeout=True, **kwargs):
        '''
        Performs a query from the keyword arguments.
        You can do the same by just iterating across the ``.filter(...)``
        query, except ``iter`` allows you to pass keywords like
        ``timeout``, ``limit``, and ``sort``.

        :param timeout: whether the query should timeout, default ``True``
        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param limit: whether to limit records to a certain number
        :param **kwargs: other keywords to pass to pymongo
        :yield: each class instance from the query
        '''
        kwargs['no_cursor_timeout'] = not timeout
        self._fix_sort_arg(kwargs)
        return (
            self.klass(d) for d in self.klass.coll.find(self.query, **kwargs)
        )

    def list(self, timeout=True, **kwargs):
        '''
        Returns a list of results rather than an iterator::

            persons = Person.filter(age=21).list()

        :param timeout: whether the query should timeout, default ``True``
        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param limit: whether to limit records to a certain number
        :param **kwargs: other keywords to pass to pymongo
        :return: a list of the instances from the query results
        '''
        return list(self.iter(timeout=timeout, **kwargs))

    def count(self, **kwargs):
        '''
        Like ``list`` but performs a count.

            >> Person.filter(color='red').count()
            10

        :param limit: whether to limit records to a certain number
        :param **kwargs: other keywords to pass to pymongo
        :return: the number of results it found
        '''
        return self.klass.coll.count(self.query, **kwargs)

    def map(self, term, **kwargs):
        '''
        Takes a term and a set of keyword argument query and returns a
        dictionary keyed by the term values, pointing to a list of records where
        its term is that value.

        For example, if there are three Person records and favorite color is
        red for two and blue for the last::

            >> Person.filter(age__gt=25).map('color')
            {
                'red': [Person(name=Joe, age=26), Person(name=Jack, age=30)],
                'blue': [Person(name=Schmoe, age=42)],
            }

        :param timeout: whether the query should timeout, default ``True``
        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param limit: whether to limit records to a certain number
        :param **kwargs: other keywords to pass to pymongo's ``find``
        :return: a dictionary of results ``{term_value: [result1, ...]}``
        '''
        curs = self.list(**kwargs)
        mapped = {}
        for result in curs:
            val = result[term]
            if val not in mapped:
                mapped[val] = []
            mapped[val].append(result)
        return mapped


class DongoCollectionMeta(type):

    def __new__(cls, name, parents, dct):
        coll_name = dct.get('collection')
        if name != 'DongoCollection':
            if not coll_name:
                raise DongoCollectionError(
                    '{name} requires `collection` class attr'.format(name=name))
            if dct.get('database'):
                dct['db'] = CLIENT[dct['database']]
            elif DATABASE_NAME is None:
                raise DongoCollectionError(
                    '{name} requires a database name since default database '
                    'name was never set with "connect()"'.format(name=name))
            else:
                dct['database'] = DATABASE_NAME
                dct['db'] = CLIENT[DATABASE_NAME]
            dct['coll'] = dct['db'][coll_name]
            if 'use_uuid' not in dct:
                dct['use_uuid'] = False
        return super(DongoCollectionMeta, cls).__new__(cls, name, parents, dct)


@six.add_metaclass(DongoCollectionMeta)
class DongoCollection(object):
    '''

    This is the base class that all your collection classes will inherit from.

    To start using dongo, you might connect then define a simple class::

        from dongo import connect, DongoCollection

        connect('mydatabase')

        class User(DongoCollection):
            collection = 'users'


        for user in User.filter(username__regex='@example.org$'):
            print('user from example.org: ' + user['username'])

        print('first 10')
        for user in User.filter().iter(limit=10, sort='username'):
            print('user: {}'.format(user['username']))

        new_user = User({'username': 'me@example.org'})
        new_user.insert()

        new_user2 = User.new(username='you@example.org')
        # automatically inserted
    '''

    def __init__(self, data):
        '''
        Creates a new instance without inserting into database yet.

        :param data: a dictionary which correlates with raw data of the document
        :return: a new instance of the class that can be inserted
        '''
        self._data = data

    def __repr__(self):
        return '{0}({1!r})'.format(self.__class__.__name__, self._data)

    def __getitem__(self, index):
        '''
        Retrieves a value from the database.

        :param index: the field to retrieve
        :return: what value was found in the document in the database
        '''
        if '.' not in index:
            return self._data[index]
        keys = index.split('.')
        val = self._data[keys[0]]
        keys = keys[1:]
        # expand the rest of the keys
        while len(keys) > 1:
            val = val[keys[0]]
            keys = keys[1:]
        return val[keys[-1]]

    def __setitem__(self, index, value):
        '''
        Sets the field value in the database, making a hit at the time it's
        called.

        :param index: the field
        :param value: the new value
        :return: None
        '''
        self._update({'$set': {index: value}})
        self._set_nested(index, value)

    def _set_nested(self, index, value):
        if '.' not in index:
            self._data[index] = value
            return
        keys = index.split('.')
        val = self._data[keys[0]]
        keys = keys[1:]
        while len(keys) > 1:
            val = val[keys[0]]
            keys = keys[1:]
        val[keys[-1]] = value

    def __contains__(self, field):
        '''
        Check if a field is in the document.

        :param field: the field name
        :return: boolean value whether document has that field
        '''
        if '.' not in field:
            return field in self._data
        keys = field.split('.')
        d = self._data
        while keys:
            if not isinstance(d, dict):
                return False
            if keys[0] not in d:
                return False
            d = d[keys[0]]
            keys = keys[1:]
        return True

    def _doc_from_db(self):
        return self.coll.find_one({'_id': self['_id']})

    def _update(self, req):
        return self.coll.update({'_id': self['_id']}, req)

    def refresh_from_db(self):
        '''
        Updates object data to most recently stored in Mongo.
        If other processes might have updated it and you absolutely need the
        latest, refresh_from_db it::

            person = Person.filter().first()
            print(person)
            import time ; time.sleep(30)
            # see if it was updated by something else
            person.refresh_from_db()
            print(person)

        :return: None
        '''
        self._data = self._doc_from_db()

    def get(self, field, default=None):
        '''
        Gets the value, or ``default``
        Will work with . operator in key.

        Example::

            # foo is {"bar": {"baz": true}}
            >> foo.get('bar.baz')
            True

        :param field: the field to check
        :param default: default to return if it doesn't exist, default None
        :return: the field value or ``default``
        '''
        if field in self:
            return self[field]
        return default

    def _translate_dunder_dict(self, kwargs):
        dct = {}
        for key, val in kwargs.items():
            if '__' in key:
                key.replace('__', '.')
            dct[key] = val
        return dct

    def set(self, **kwargs):
        '''
        Performs a pymongo update using $set::

            person = Person.filter().first()
            person.set(name='new name', age=30)

        :param **kwargs: each new field to update
        :return: the pymongo ``update`` result
        '''
        dct = self._translate_dunder_dict(kwargs)
        for key, val in dct.items():
            self._set_nested(key, val)
        self._update({'$set': dct})

    def inc(self, field, amt=1):
        '''
        Performs an increment with an optional amount, default 1::

            person = Person.filter().first()
            person.inc('age')
            person.inc('money', amt=100)

        :param field: the field to increment
        :param amt: the amount to increment, default 1
        :return: the result of the pymongo $inc ``update``
        '''
        upd = self._update({'$inc': {field: amt}})
        val = self[field]
        self._set_nested(field, val + amt)
        return upd

    @classmethod
    def get_one(cls, **query):
        '''
        Performs a lookup to retrieve one record matching the query, like
        Django's ``get``, as in ``MyModel.objects.get(...)``.
        Raises exception if more than one instance is returned, like Django.

        :param **query: the query to filter on
        :return: the instance retrieved
        :raises: ``DongoResultError``
        '''
        results = list(cls.filter(**query))
        if len(results) > 1:
            raise DongoResultError('{0}.get_one returned more than 1 instance '
                                   '({1})'.format(cls.__name__, len(results)))
        return results[0]

    @classmethod
    def new(cls, **data):
        '''
        Instantly creates and inserts the new record.

        Example::

            person = Person.new({'name': 'Joe', 'age': 100})

        '''
        new_item = cls(data)
        new_item.insert()
        return new_item

    @classmethod
    def _translate_opt(cls, key, val):
        key = key[2:]
        if key in KEYWORD_TRANSLATIONS:
            return KEYWORD_TRANSLATIONS[key], val
        elif key == 'timeout':
            return 'no_cursor_timeout', not bool(val)
        else:
            return key, val

    @classmethod
    def _build_query_from_term(cls, term, val):
        if '__' not in term:
            return term, val
        m = RE_COMP_EXPR.match(term)
        op = None
        if m:
            term, op = m.groups()
        term = term.replace('__', '.')
        if op is None:
            return term, val
        return term, {'${0}'.format(op): val}

    @classmethod
    def _build_query(cls, kwargs):
        query = {}
        opts_kwargs = {}
        for k, v in kwargs.items():
            if k.startswith('__'):
                new_key, new_val = cls._translate_opt(k, v)
                opts_kwargs[new_key] = new_val
                continue
            term, val = cls._build_query_from_term(k, v)
            query[term] = val
        return query, opts_kwargs

    @classmethod
    def by_uuid(cls, instance_uuid, **kwargs):
        '''
        Find the record with the uuid value::

            p = Person.by_uuid('13bef77b-2a36-4d59-9339-42a5aa098833')

        :param instance_uuid: the uuid to check
        :return: the class instance or None
        '''
        real_uuid = to_uuid(instance_uuid)
        return cls.filter(_uuid=real_uuid).first(**kwargs)

    @classmethod
    def by_uuids(cls, uuids, **kwargs):
        '''
        Find many records from a list of uuids::

            persons = Person.by_uuids([
                '13bef77b-2a36-4d59-9339-42a5aa098833',
                ...,
            ])

        :param uuids: the uuids to check
        :return: the class instances
        '''
        uuid_list = [to_uuid(g) for g in uuids]
        return cls.filter(_uuid__in=uuid_list).list(**kwargs)

    @classmethod
    def map_by_uuids(cls, uuids, **kwargs):
        '''
        Find many records from a list of uuids, and returns a map mapping passed
        uuid to record (or None if not found)::

            person_map = Person.by_uuids([
                '13bef77b-2a36-4d59-9339-42a5aa098833',
                ...,
            ])
            first_person = person_map['13bef77b-2a36-4d59-9339-42a5aa098833']

        :param uuids: the uuids to check
        :return: a dictionary mapping uuid => person or None
        '''
        uuid_map = {to_uuid(g): g for g in uuids}
        lst = cls.filter(
            _uuid__in=list(uuid_map.keys())
        ).list(**kwargs)
        results = {i_uuid: None for i_uuid in uuids}
        for record in lst:
            orig_uuid = uuid_map[record['_uuid']]
            results[orig_uuid] = record
        return results

    @classmethod
    def by_id(cls, _id, **kwargs):
        '''
        Find the record with the bson ``ObjectId`` value::

            p = Person.by_id('6725b84b2401323bfda626e7')

        :param _id: the ``ObjectId`` or string object id to check
        :return: the class instance or None
        '''
        if isinstance(_id, six.string_types):
            _id = ObjectId(_id)
        return cls.filter(_id=_id).first(**kwargs)

    @classmethod
    def by_ids(cls, ids, **kwargs):
        '''
        Find the records with the bson ``ObjectId`` values::

            persons = Person.by_ids(['6725b84b2401323bfda626e7', ...])

        :param ids: the ``ObjectId`` or string object id list to check
        :return: the class instances
        '''
        id_list = [
            ObjectId(x) if isinstance(x, six.string_types) else x
            for x in ids
        ]
        return cls.filter(_id__in=id_list).list(**kwargs)

    @classmethod
    def map_by_ids(cls, ids, **kwargs):
        '''
        Find many records from a list of ids, and returns a map mapping passed
        id to record (or None if not found)::

            person_map = Person.by_uuids([
                '6725b84b2401323bfda626e7',
                ...,
            ])
            first_person = person_map['6725b84b2401323bfda626e7']

        :param uuids: the uuids to check
        :return: a dictionary mapping uuid => person or None
        '''
        id_map = {
            ObjectId(_id) if isinstance(_id, six.string_types) else _id: _id
            for _id in ids
        }
        lst = cls.filter(
            _id__in=list(id_map.keys())
        ).list(**kwargs)
        results = {_id: None for _id in ids}
        for record in lst:
            orig_id = id_map[record['_id']]
            results[orig_id] = record
        return results

    def _unique_uuid(self):
        # Use timestamp generated _id from mongodb
        if self['_id']:
            return self._uuid_from_id(self['_id'])
        # very, very, likely unique, but only should use _id if possible
        return uuid4()

    @classmethod
    def _uuid_from_id(cls, _id):
        s = sha256(str(_id).encode('utf8')).digest()
        return UUID(bytes=s[:16])

    def _insert_uuid(self):
        if '_uuid' not in self._data:
            self['_uuid'] = self._unique_uuid()
        return self['_uuid']

    @classmethod
    def bulk_create(cls, objs, use_uuid=None, **kwargs):
        '''
        Insert many records into the database at once::

            Person.bulk_create([
                {'name': 'joe', 'age': 21},
                {'name': 'bob', 'age': 22},
                ...
            ])
            person1 = Person({'name': 'greg': 'age': 38})
            person2 = Person({'name': 'bill': 'age': 45})
            Person.bulk_create([person1, person2, ...])

        :param objs: the dictionaries or class instances to insert in bulk
        :return: the pymongo ``insert_many`` result
        '''
        _, opts = cls._build_query(kwargs)
        if not objs:
            return []
        if isinstance(objs[0], dict):
            objs = [cls(x) for x in objs]
        inserted_ids = cls.coll.insert_many(
            [x._data for x in objs]).inserted_ids
        if use_uuid is None:
            use_uuid = cls.use_uuid
        updates = []
        for i, x in enumerate(objs):
            _id = inserted_ids[i]
            x._data['_id'] = _id
            if use_uuid is not False:
                i_uuid = x._uuid_from_id(_id)
                x._data['_uuid'] = i_uuid
                updates.append(UpdateOne(
                    {'_id': _id}, {'$set': {'_uuid': i_uuid}}
                ))
        if updates:
            cls.coll.bulk_write(updates, bypass_document_validation=True,
                                **opts)
        return inserted_ids

    def insert(self, use_uuid=None, **kwargs):
        '''
        Insert the class instance into the database::

            p = Person({'name': 'joe', 'age': 21})
            p.insert()

        :return: the ``ObjectId`` of the new database record
        '''
        _, opts = self._build_query(kwargs)
        self._data['_id'] = self.coll.insert_one(self._data, **opts).inserted_id
        if use_uuid is None:
            use_uuid = self.__class__.use_uuid
        if use_uuid is not False:
            self._insert_uuid()
        return self._data['_id']

    def json(self, datetime_format=None):
        '''
        Serialize the object into JSON serializable data::

            >> p = Person.new(name='joe', birthday=datetime(2000, 2, 1))
            >> print(p.json())
            {'name': 'joe', 'birthday': '2000-02-01T00:00:00.000000'}
            >> print(p.json(datetime_format='%d/%m/%Y')
            {'name': 'joe', 'birthday': '01/02/2000'}

        :param datetime_format: the strftime datetime format string to use, or
            it just uses ``isoformat()``
        :return: the json serializable dictionary
        '''
        return recursive_serialize(self._data, datetime_format=datetime_format)

    def delete(self, **kwargs):
        '''
        Delete the instance from the db::

            >> p = Person.filter(name='joe').first()
            >> p.delete()
            >> p = Person.filter(name='joe').first()
            >> print(p)
            None

        :return: the pymongo ``delete_one`` result
        '''
        if not self.get('_id'):
            return
        return self.coll.delete_one({'_id': self['_id']}, **kwargs)

    @classmethod
    def filter(cls, **query):
        '''
        Create a ``QuerySet`` based on the query::

            for p in Person.filter(age__gte=21):
                print(p['name'] + ' can drink')

        :param **query: the query to filter on
        :return: the ``QuerySet`` instance
        '''
        return QuerySet(cls, query)

    @classmethod
    def filter_or(cls):
        '''
        Creates a ``QuerySet`` with a top level $or, which you append to::

            # Finds person where their age is 18 or 21
            # Logically the same as Person.filter(age__in=[18, 21])
            ms = Person.filter_or()
            ms += Person.filter(age=21)
            ms += Person.filter(age=18)

        :return: the ``QuerySet`` instance
        '''
        return QuerySet._filter_or(cls)

    @classmethod
    def filter_and(cls):
        '''
        Creates a ``QuerySet`` with a top level $and, which you append to::

            # Finds person where their age is 18 and name is 'Joe'
            # Logically the same as Person.filter(name='Joe', age=18)
            ms = Person.filter_and()
            ms += Person.filter(name='Joe')
            ms += Person.filter(age=18)

        :return: the ``QuerySet`` instance
        '''
        return QuerySet._filter_and(cls)

    @classmethod
    def create_index(cls, *args, **kwargs):
        '''
        Create an index on the collection, default in the background::

            Person.create_index('name')
            ...
            Person.create_index([('name', 1), ('age', -1)])

        :param *args: the string term to index on, or a list of
                      (term, direction) tuples
        :param background: whether to create the index in the background
            (default: True)
        :return: the pymongo ``create_index`` result
        '''
        # Python 2 doesn't like keyword arguments after *args and before
        # **kwargs.
        background = kwargs.get('background', True)
        return cls.db.create_index(*args, background=background, **kwargs)


def recursive_serialize(data, datetime_format=None):
    if isinstance(data, dict):
        copy = {}
        for k, v in data.items():
            copy[k] = recursive_serialize(v, datetime_format=datetime_format)
        return copy
    if isinstance(data, (list, tuple)):
        copy = []
        for item in data:
            copy.append(
                recursive_serialize(item, datetime_format=datetime_format)
            )
        return copy
    if isinstance(data, six.string_types):
        return data
    if isinstance(data, (int, float, bool)):
        return data
    if data is None:
        return data
    if isinstance(data, datetime):
        if datetime_format is None:
            return data.isoformat()
        return data.strftime(datetime_format)
    if isinstance(data, UUID):
        return str(data)
    return str(data)
