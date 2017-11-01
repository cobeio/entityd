import collections

import entityd.groups


class TestGroup:

    def test(self):
        group = entityd.groups.group('foo', 'bar')
        assert group.metype == 'Group'
        assert len(list(group.attrs)) == 2
        assert group.attrs.get('kind').value == 'foo'
        assert group.attrs.get('kind').traits == {'entity:id'}
        assert group.attrs.get('id').value == 'bar'
        assert group.attrs.get('id').traits == {'entity:id'}
        assert not group.parents
        assert not group.children

    def test_kind_not_string(self):
        group = entityd.groups.group(420, 'bar')
        assert group.metype == 'Group'
        assert len(list(group.attrs)) == 2
        assert group.attrs.get('kind').value == '420'
        assert group.attrs.get('kind').traits == {'entity:id'}
        assert group.attrs.get('id').value == 'bar'
        assert group.attrs.get('id').traits == {'entity:id'}
        assert not group.parents
        assert not group.children

    def test_id_not_string(self):
        group = entityd.groups.group('foo', 420)
        assert group.metype == 'Group'
        assert len(list(group.attrs)) == 2
        assert group.attrs.get('kind').value == 'foo'
        assert group.attrs.get('kind').traits == {'entity:id'}
        assert group.attrs.get('id').value == '420'
        assert group.attrs.get('id').traits == {'entity:id'}
        assert not group.parents
        assert not group.children


class TestLabels:

    def test(self):
        groups = list(entityd.groups.labels(collections.OrderedDict([
            ('foo', 'bar'),
            ('spam', 'eggs'),
        ])))
        assert len(groups) == 2
        assert groups[0].metype == 'Group'
        assert len(list(groups[0].attrs)) == 2
        assert groups[0].attrs.get('kind').value == 'label:foo'
        assert groups[0].attrs.get('kind').traits == {'entity:id'}
        assert groups[0].attrs.get('id').value == 'bar'
        assert groups[0].attrs.get('id').traits == {'entity:id'}
        assert not groups[0].parents
        assert not groups[0].children
        assert groups[1].metype == 'Group'
        assert len(list(groups[1].attrs)) == 2
        assert groups[1].attrs.get('kind').value == 'label:spam'
        assert groups[1].attrs.get('kind').traits == {'entity:id'}
        assert groups[1].attrs.get('id').value == 'eggs'
        assert groups[1].attrs.get('id').traits == {'entity:id'}
        assert not groups[1].parents
        assert not groups[1].children

    def test_key_not_string(self):
        group, = entityd.groups.labels({420: 'bar'})
        assert group.metype == 'Group'
        assert len(list(group.attrs)) == 2
        assert group.attrs.get('kind').value == 'label:420'
        assert group.attrs.get('kind').traits == {'entity:id'}
        assert group.attrs.get('id').value == 'bar'
        assert group.attrs.get('id').traits == {'entity:id'}
        assert not group.parents
        assert not group.children

    def test_value_not_string(self):
        group, = entityd.groups.labels({'foo': 420})
        assert group.metype == 'Group'
        assert len(list(group.attrs)) == 2
        assert group.attrs.get('kind').value == 'label:foo'
        assert group.attrs.get('kind').traits == {'entity:id'}
        assert group.attrs.get('id').value == '420'
        assert group.attrs.get('id').traits == {'entity:id'}
        assert not group.parents
        assert not group.children
