from optparse import make_option

from django.core.management.base import BaseCommand
from django.utils.translation import ugettext as _

from django_q.cluster import Cluster
from django_q.brokers import get_broker_for_queue


class Command(BaseCommand):
    # Translators: help text for qcluster management command
    help = _("Starts a Django Q Cluster.")

    option_list = BaseCommand.option_list + (
        make_option('--run-once',
                    action='store_true',
                    dest='run_once',
                    default=False,
                    help='Run once and then stop.'),
        make_option('--queue',
                    dest='queue',
                    default='default',
                    help='Queue name.'),
    )

    def handle(self, *args, **options):
        q = Cluster(get_broker_for_queue(options.get('queue')))
        q.start()
        if options.get('run_once', False):
            q.stop()
