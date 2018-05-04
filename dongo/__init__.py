'''
dongo

A Django-ORM inspired Mongo ODM.
'''
from .odm import (
    connect, QuerySet, DongoCollection, DongoClient, to_uuid, deref,
    deref_single, deref_many,
)
from .exceptions import (
    DongoError, DongoConnectError, DongoResultError, DongoCollectionError,
    DongoDerefError,
)


__title__ = 'dongo'
__version__ = '0.3.0'
__all__ = ('connect', 'QuerySet', 'DongoCollection', 'DongoError',
           'DongoConnectError', 'DongoResultError', 'DongoCollectionError',
           'DongoClient', 'to_uuid', 'DongoDerefError', 'deref', 'deref_single',
           'deref_many')
__author__ = 'Johan Nestaas <johannestaas@gmail.com>'
__license__ = 'LGPLv3+'
__copyright__ = 'Copyright 2018 Johan Nestaas'
