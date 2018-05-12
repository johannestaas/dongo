from dongo import DongoCollection, connect


connect('dongotest')


class Person(DongoCollection):
    '''
    Example of a Person::

        {
            "name": "joe",
            "age": 20,
            "color": "green"
        }
    '''
    collection = 'persons'

    @classmethod
    def output(cls):
        print('outputting all persons:')
        for inst in cls.filter():
            print('  name={name} age={age} color={color}'.format(**inst.json()))


def populate():
    p1 = Person.new(name='joe', age=20, color='red', delete_me=True)
    Person.new(name='john', age=30, color='green', delete_me=True)
    Person.new(name='jack', age=40, color='blue', delete_me=True)
    Person.new(name='dill', age=50, color='blue', delete_me=True)
    print('made persons, first is: {}'.format(p1['name']))


def main():
    Person.filter(delete_me=True).delete()
    print('populating persons')
    populate()

    print('making bulk updater')
    bulk = Person.bulk()

    print('doing lazy update of incrementing all ages + 1')
    for person in Person.filter():
        bulk.inc_one(person, age=1)

    print('getting DongoLazyUpdaters from every person')
    lazies = [
        person.lazy()
        for person in Person.filter()
    ]

    print('setting all favorite colors to purple!')
    for lazy in lazies:
        lazy['color'] = 'PURPLE!'
        bulk += lazy

    print('before performing the bulk save:')
    Person.output()

    bulk.save()

    print('after bulk.save():')
    Person.output()

    print('{} persons exist... deleting.'.format(Person.filter().count()))
    Person.filter(delete_me=True).delete()
    print('{} persons exist!'.format(Person.filter().count()))


if __name__ == '__main__':
    main()
