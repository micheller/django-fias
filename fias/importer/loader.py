#coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
from django.conf import settings
from django import db
from django.db import transaction
from progress.helpers import WritelnMixin
from sys import stderr

from .validators import validators


class LoadingBar(WritelnMixin):
    file = stderr

    text = 'Table: %(table)s.' \
           ' Loaded: %(loaded)d | Updated: %(updated)d | Skipped:  %(skipped)d' \
           ' \t\t\tFilename: %(filename)s'

    loaded = 0
    updated = 0
    skipped = 0
    hide_cursor = False

    def __init__(self, message=None, **kwargs):
        self.table = kwargs.pop('table', 'unknown')
        self.filename = kwargs.pop('filename', 'unknown')
        super(LoadingBar, self).__init__(message=message, **kwargs)

    def __getitem__(self, key):
        if key.startswith('_'):
            return None
        return getattr(self, key, None)

    def update(self, loaded=0, updated=0, skipped=0):
        if loaded:
            self.loaded = loaded
        if updated:
            self.updated = updated
        if skipped:
            self.skipped = skipped

        ln = self.text % self
        self.writeln(ln)


class TableLoader(object):

    def __init__(self, limit=10000):
        self.limit = int(limit)
        self.today = datetime.date.today()

    def validate(self, table, item):
        if item is None or item.pk is None:
            return False

        return validators.get(table.name, lambda x, **kwargs: True)(item, today=self.today)

    @staticmethod
    def create(table, objects):
        table.model.objects.bulk_create(objects)

        if settings.DEBUG:
            db.reset_queries()

    def load(self, tablelist, table):
        bar = LoadingBar(table=table.name, filename=table.filename)
        new_counter = 0
        skip_counter = 0

        objects = []
        for item in table.rows(tablelist=tablelist):
            if not self.validate(table, item):
                skip_counter += 1
                continue

            objects.append(item)
            new_counter += 1

            if new_counter and new_counter % self.limit == 0:
                self.create(table, objects)
                objects = []
                bar.update(loaded=new_counter, skipped=skip_counter)

        if objects:
            self.create(table, objects)
            bar.update(loaded=new_counter, skipped=skip_counter)

        bar.update(skipped=skip_counter)
        bar.finish()


class TableUpdater(TableLoader):
    """

        Updating started at 2015-11-29 10:27:30.437976
        INFO: Updating table `socrbase` from 1000 to 1000 from 2015-05-05...
        Table: socrbase. Loaded: 0 | Updated: 259 | Skipped:  2 			Filename: AS_SOCRBASE_20151112_c8062afa-0757-44ba-8bb0-47075f1ffaad.XML
        INFO: Updating table `addrobj` from 1000 to 1000 from 2015-05-05...
        Table: addrobj. Loaded: 0 | Updated: 2175 | Skipped:  5054 			Filename: AS_ADDROBJ_20151112_7e9b5ce1-3da6-4cd7-89e2-81c002d7d4ab.XML
        INFO: Updating table `house` from 1000 to 1000 from 2015-05-05...
        Table: house. Loaded: 0 | Updated: 39708 | Skipped:  59513 			Filename: AS_HOUSE_20151112_618fab88-9e6b-4c09-861e-031382443782.XML
        Updating ended at 2015-11-29 10:36:04.424718
        Estimated time: 0:08:33.986742


    """

    def __init__(self, limit=10000):
        self.upd_limit = limit / 10
        super(TableUpdater, self).__init__(limit=limit)

    def load(self, tablelist, table):
        bar = LoadingBar(table=table.name, filename=table.filename)
        new_counter = 0
        upd_counter = 0
        skip_counter = 0

        model = table.model
        objects = []
        for item in table.rows(tablelist=tablelist):
            if not self.validate(table, item):
                skip_counter += 1
                continue

            try:
                old_obj = model.objects.get(pk=item.pk)
            except model.DoesNotExist:
                objects.append(item)
                new_counter += 1
            else:
                #if not hasattr(item, 'updatedate') or old_obj.updatedate < item.updatedate:
                item.save(force_update=True)
                upd_counter += 1

            if new_counter and new_counter % self.limit == 0:
                self.create(table, objects)
                objects = []
                bar.update(loaded=new_counter, updated=upd_counter, skipped=skip_counter)

            if upd_counter and upd_counter % self.upd_limit == 0:
                bar.update(loaded=new_counter, updated=upd_counter, skipped=skip_counter)

        if objects:
            self.create(table, objects)

        bar.update(loaded=new_counter, updated=upd_counter, skipped=skip_counter)
        bar.finish()


class NewTableUpdater(TableLoader):
    """
    Updating started at 2015-11-29 10:36:22.249523
    INFO: Updating table `socrbase` from 1000 to 1000 from 2015-05-05...
    Table: socrbase. Loaded: 0 | Updated: 259 | Skipped:  2 			Filename: AS_SOCRBASE_20151112_c8062afa-0757-44ba-8bb0-47075f1ffaad.XML
    INFO: Updating table `addrobj` from 1000 to 1000 from 2015-05-05...
    Table: addrobj. Loaded: 0 | Updated: 2175 | Skipped:  5054 			Filename: AS_ADDROBJ_20151112_7e9b5ce1-3da6-4cd7-89e2-81c002d7d4ab.XML
    INFO: Updating table `house` from 1000 to 1000 from 2015-05-05...
    Table: house. Loaded: 0 | Updated: 39708 | Skipped:  59513 			Filename: AS_HOUSE_20151112_618fab88-9e6b-4c09-861e-031382443782.XML
    Updating ended at 2015-11-29 10:45:18.586784
    Estimated time: 0:08:56.337261
    """

    def split_objects(self, table, objects):
        ids = [item.pk for item in objects]

        exists_ids = set(table.model.objects.filter(pk__in=ids).values_list('pk', flat=True))

        new_objects = []
        exists_objects = []
        for item in objects:
            if item._meta.pk.to_python(item.pk) in exists_ids:
                exists_objects.append(item)
            else:
                new_objects.append(item)

        return new_objects, exists_objects

    @transaction.atomic(savepoint=True)
    def update(self, table, objects):
        for obj in objects:
            obj.save(force_update=True)

    def load(self, tablelist, table):
        bar = LoadingBar(table=table.name, filename=table.filename)
        counter = 0
        new_counter = 0
        upd_counter = 0
        skip_counter = 0

        objects = []
        for item in table.rows(tablelist=tablelist):
            if not self.validate(table, item):
                skip_counter += 1
                continue

            objects.append(item)
            counter += 1
            if counter and counter % self.limit == 0:
                new_objects, exists_objects = self.split_objects(table, objects)
                new_counter += len(new_objects)
                upd_counter += len(exists_objects)

                self.create(table, new_objects)
                self.update(table, exists_objects)

                objects = []
                bar.update(loaded=new_counter, updated=upd_counter, skipped=skip_counter)

        if objects:
            new_objects, exists_objects = self.split_objects(table, objects)
            new_counter += len(new_objects)
            upd_counter += len(exists_objects)

            self.create(table, new_objects)
            self.update(table, exists_objects)

        del objects
        bar.update(loaded=new_counter, updated=upd_counter, skipped=skip_counter)
        bar.finish()
