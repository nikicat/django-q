from django_q.cluster import Cluster
from django_q.monitor import monitor, Stat


def test_monitor():
    assert Stat.get(0).sentinel == 0
    c = Cluster()
    c.start()
    stats = monitor(run_once=True)
    c.stop()
    assert len(stats) > 0
    found_c = False
    for stat in stats:
        if stat.cluster_id == c.pid:
            found_c = True
            assert stat.uptime() > 0
            assert stat.empty_queues() is True
            break
    assert found_c is True
