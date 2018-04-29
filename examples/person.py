from dongo import DongoCollection, connect
from datetime import datetime


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

    def print_name(self):
        print(self.get('name', 'unknown'))

    def serialize(self):
        return {
            'name': self.get('name'),
            'age': self.get('age', 0),
            'birthday': self.get('start', datetime.min).isoformat(),
            'color': self.get('color'),
        }

    def change_color(self, new_color):
        # updates record in database as well
        self['color'] = new_color

    @classmethod
    def start_new_year(cls):
        # add 1 to all age values for every record with a field "age"
        cls.filter(age__exists=1).inc(age=1)
        # kill off those 110 and older
        cls.filter(age__gte=110).delete()

    @classmethod
    def startswith(cls, prefix):
        # find all persons with a name that starts with ``prefix``
        regex = '^{}'.format(prefix)
        return cls.filter(name__regex=regex)

    @classmethod
    def endswith(cls, suffix):
        # find all persons with a name that ends with ``suffix``
        regex = '{}$'.format(suffix)
        return cls.filter(name__regex=regex)

    @classmethod
    def first_10(cls):
        return cls.filter().iter(limit=10, sort='name')

    @classmethod
    def sort_by_oldest_first_then_alphabetically(cls):
        return cls.filter().iter(sort=[('age', -1), ('name', 1)])


def populate():
    p1 = Person.new(name='joe', age=20, color='red')
    Person.new(name='john', age=35, color='green')
    Person.new(name='jack', age=30, color='blue')
    Person.new(name='dill', age=25, color='blue')
    print('made persons, first is: {}'.format(p1['name']))


def main():
    if Person.filter().count() < 4:
        print('populating persons')
        populate()

    for p in Person.filter():
        print(p)

    for p in Person.startswith('j'):
        print('starts with j: {} (age {})'.format(p['name'], p['age']))

    print('aging them!')
    Person.start_new_year()

    for p in Person.filter(color='blue'):
        print('{} (age {}) likes {}'.format(
            p['name'], p['age'], p['color'],
        ))


if __name__ == '__main__':
    main()
