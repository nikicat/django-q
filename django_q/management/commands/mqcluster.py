import signal

from django.core.management.base import BaseCommand
from django.utils.translation import ugettext as _

from django_q.conf import Conf
from django_q.cluster import Cluster
from django_q.brokers import get_broker_for_queue


class Command(BaseCommand):
    # Translators: help text for mqcluster management command
    help = _("Starts a Django Q Multi-queue Cluster.")

    def handle(self, *args, **options):
        self.clusters = []
        for name, conf in Conf.QUEUES.items():
            q = Cluster(broker=get_broker_for_queue(name),
                        workers=conf['workers'])
            q.start()
            self.clusters.append(q)

        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)

    def sig_handler(self, signum, frame):
        for cluster in self.clusters:
            cluster.stop()
