"""Fast, network-free checks for the independent-leader capacity experiment."""
import json
import sys
import unittest
from pathlib import Path

HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
import run_parallel_instances as experiment
import run_suite as suite
import isolated_cluster as cluster

TOPO=json.loads((HERE/'topology.example.json').read_text())


class ParallelInstanceTests(unittest.TestCase):
    def test_constant_total_workers_and_partitioned_keyspace(self):
        for workers_per_cn in (5,8):
            for n in (1,5):
                with self.subTest(instances=n,per_cn=workers_per_cn):
                    cases=experiment.make_cases(TOPO,n,1,'TEST_GO',
                                                workers_per_cn)
                    self.assertEqual(len(cases),n)
                    self.assertEqual(len({c['id'] for c in cases}),n)
                    self.assertEqual(sum(c['locks'] for c in cases),100000)
                    self.assertEqual(sum(c['workers_per_cn']*c['cn_count']
                                         for c in cases),2*workers_per_cn)
                    self.assertEqual({c['measure_ms'] for c in cases},{1000})
                    self.assertEqual({c['warmup_ms'] for c in cases},{0})
                    self.assertEqual({c['data_ops'] for c in cases},{1})
                    self.assertEqual({c['distribution'] for c in cases},{'uniform'})
                    self.assertEqual({c['transport'] for c in cases},{'rdma'})
                    self.assertEqual({c['profile'] for c in cases},{'raw'})
                    self.assertEqual({c['probe'] for c in cases},{0})
                    self.assertEqual({c['handoff_preconnect'] for c in cases},{0})
                    self.assertEqual({c['paxos_slots'] for c in cases},{131072})
                    self.assertEqual({c['max_ops'] for c in cases},{90000})
                    self.assertEqual(
                        [c['port_offset'] for c in cases],
                        ([1000] if n==1 else [200*i for i in range(n)]))
                    self.assertEqual({c['port_base'] for c in cases},{16560})
                    for c in cases:
                        cfg=suite.case_topology(TOPO,c,4101)
                        self.assertEqual(cfg['benchmark']['start_gate'],'TEST_GO')
                        self.assertEqual(cluster.validate(cfg),
                                         2*c['workers_per_cn'])
                    experiment.port_sets(TOPO,cases)
        five=experiment.make_cases(TOPO,5,1,'TEST_GO',8)
        self.assertEqual([c['workers_per_cn'] for c in five],[2,2,2,1,1])
        self.assertEqual([c['locks'] for c in five],
                         [25000,25000,25000,12500,12500])
        old=experiment.make_cases(TOPO,5,1,'TEST_GO',5)
        self.assertEqual([c['workers_per_cn'] for c in old],[1]*5)
        self.assertEqual([c['locks'] for c in old],[20000]*5)

    def test_refuses_bad_gate_and_insufficient_cores(self):
        for gate in ('../escape','abc space','', 'x'*97):
            with self.subTest(gate=gate):
                with self.assertRaises(ValueError):
                    experiment.make_cases(TOPO,5,1,gate)
        for count in (0,2,6):
            with self.subTest(count=count):
                with self.assertRaises(ValueError):
                    experiment.make_cases(TOPO,count,1,'VALID')
        other=json.loads(json.dumps(TOPO))
        other['clients'][0]['cores']=4
        with self.assertRaisesRegex(ValueError,'8 usable cores'):
            experiment.make_cases(other,5,1,'VALID')
        for count in (4,9,17,True):
            with self.subTest(workers_per_cn=count):
                with self.assertRaises(ValueError):
                    experiment.make_cases(TOPO,5,1,'VALID',count)

    def test_ports_are_disjoint_on_shared_hosts(self):
        cases=experiment.make_cases(TOPO,5,1,'VALID')
        ports=experiment.port_sets(TOPO,cases)
        self.assertEqual(len(ports),5)
        self.assertEqual(sum(map(len,ports)),len(set().union(*ports)))
        self.assertEqual([suite.case_topology(TOPO,c,4101)['ports']['ordering']
                          for c in cases],[16560,16760,16960,17160,17360])
        duplicated=json.loads(json.dumps(cases))
        duplicated[1]['port_offset']=0
        with self.assertRaisesRegex(ValueError,'overlap'):
            experiment.port_sets(TOPO,duplicated)

    def test_no_port_reuse_between_baseline_five_instances_or_repetitions(self):
        all_ranges=[]
        # Six services per repetition (one control + five concurrent)
        # receive distinct ports. None of our ports overlap typical Linux
        # ephemeral SSH source ports (32768..60999), even in round ten.
        for rep in range(1,11):
            for count in (1,5):
                cases=experiment.make_cases(TOPO,count,rep,'VALID',8)
                all_ranges.extend(experiment.port_sets(TOPO,cases))
        union=set().union(*all_ranges)
        self.assertEqual(len(union),sum(map(len,all_ranges)))
        self.assertLess(max(union),32768)
        self.assertGreaterEqual(min(union),1024)
        first_baseline=experiment.make_cases(TOPO,1,1,'VALID')[0]
        first_five=experiment.make_cases(TOPO,5,1,'VALID')
        second_five=experiment.make_cases(TOPO,5,2,'VALID')
        self.assertEqual(first_baseline['port_offset'],1000)
        self.assertEqual([c['port_offset'] for c in first_five],
                         [0,200,400,600,800])
        self.assertEqual([c['port_offset'] for c in second_five],
                         [1200,1400,1600,1800,2000])

    def test_bad_parallel_ports_and_gate_rejected_in_topology(self):
        c=experiment.make_cases(TOPO,5,1,'VALID')[0]
        for offset in (-1,100,12001,12200,True):
            with self.subTest(offset=offset):
                bad=dict(c,port_offset=offset)
                with self.assertRaises(ValueError):
                    suite.case_topology(TOPO,bad,4101)
        for base in (-1,0,True,'16560',51001):
            with self.subTest(port_base=base):
                bad=dict(c,port_base=base)
                with self.assertRaises(ValueError):
                    suite.case_topology(TOPO,bad,4101)
        for token in ('../bad','bad/yes','bad.txt','',123):
            with self.subTest(token=token):
                bad=dict(c,start_gate=token)
                with self.assertRaises(ValueError):
                    suite.case_topology(TOPO,bad,4101)

    def test_aggregate_never_counts_failed_partial_or_unsynchronized(self):
        def row(i,n,start):
            return dict(status='PASS',go_epoch_ns=str(start),
                        worker_processes=10//n,lock_count=100000//n,
                        measure_ms=1000,warmup_ms=0,goodput_ops_per_sec='12345.5',
                        latency_p99_ns='150000',measured=12345)
        start=1800000000000000000
        for n in (1,5):
            rows=[row(i,n,start+i*1200000) for i in range(n)]
            result=experiment.combine(n,1,rows,5)
            self.assertEqual(result['instances'],n)
            self.assertEqual(result['workers_total'],10)
            self.assertEqual(result['keys_total'],100000)
            self.assertAlmostEqual(result['aggregate_ops_per_sec'],n*12345.5)
        # Default 16 clients are allocated unevenly but keep total
        # workers, 100K keys, and synchronized GO exactly identical.
        cases=experiment.make_cases(TOPO,5,1,'VALID',8)
        rows16=[dict(row(i,5,start+i*1200000),
                     worker_processes=2*c['workers_per_cn'],
                     lock_count=c['locks']) for i,c in enumerate(cases)]
        self.assertEqual(experiment.combine(5,1,rows16)['workers_total'],16)
        self.assertEqual(experiment.combine(5,1,rows16)['keys_total'],100000)
        for wrong in (
            [row(i,5,start+i*30000000) for i in range(5)],
            [dict(row(i,5,start),status='FAIL') if i==2 else row(i,5,start)
             for i in range(5)],
            [dict(row(i,5,start),worker_processes=8) if i==0 else row(i,5,start)
             for i in range(5)],
            [row(i,5,start) for i in range(4)],
        ):
            with self.assertRaises(ValueError):
                experiment.combine(5,1,wrong,5)

    def test_go_timestamp_in_live_parser(self):
        import run_suite
        from test_live_suite import LIVE_LINE
        raw=LIVE_LINE+' go_epoch_ns=1800000000000000000'
        self.assertEqual(run_suite.extract_live(raw)['go_epoch_ns'],
                         '1800000000000000000')
        with self.assertRaisesRegex(ValueError,'invalid LIVE GO timestamp'):
            run_suite.extract_live(LIVE_LINE+' go_epoch_ns=broken')


if __name__=='__main__':
    unittest.main()
