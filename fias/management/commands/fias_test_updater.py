#coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
import sys
from optparse import make_option

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.translation import activate

from fias.importer.source import TableListLoadingError
from fias.importer.commands import auto_update_data, load_complete_data, update_data, new_update_data


class Command(BaseCommand):
    help = 'Fill or update FIAS database'
    usage_str = 'Usage: ./manage.py fias_test_updater --src1 <path|filename> --src2 <path|filename> --new'

    option_list = BaseCommand.option_list + (
        make_option('--src1', action='store', dest='src1', default=None,
                    help='Load dir|file into DB.'),
        make_option('--src2', action='store', dest='src2', default=None,
                    help='Then update DB with dir|file'),

        make_option('--new', action='store_true', dest='new', default=False,
                    help='Use new updater'),

    )

    def handle(self, *args, **options):
        src1 = options.pop('src1')
        src2 = options.pop('src2')
        use_new = options.pop('new')
        tables = ['socrbase', 'addrobj', 'house']

        # Force Russian language for internationalized projects
        if settings.USE_I18N:
            activate('ru')

        if src1:
            start_load = datetime.datetime.now()
            try:

                print('Loading started at {0}'.format(start_load))
                load_complete_data(path=src1, truncate=True)

                end_load = datetime.datetime.now()
                print('Loading ended at {0}'.format(end_load))
            except TableListLoadingError as e:
                self.error(str(e))
            else:
                print('Estimated time: {0}'.format(end_load - start_load))

        if src2:
            start_update = datetime.datetime.now()
            try:

                print('Updating started at {0}'.format(start_update))
                if use_new:
                    new_update_data(path=src2, tables=tables)
                else:
                    update_data(path=src2, tables=tables)

                end_update = datetime.datetime.now()
                print('Updating ended at {0}'.format(end_update))
            except TableListLoadingError as e:
                self.error(str(e))
            else:
                print('Estimated time: {0}'.format(end_update - start_update))

    def error(self, message, code=1):
        print(message)
        sys.exit(code)
