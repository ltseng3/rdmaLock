"""No hardware required: validate scientific ablations and runner contracts."""
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import run_suite as runner
import isolated_cluster as cluster
from profiles import PROFILES, ABLATIONS, profile

TOPO=json.loads((HERE/'topology.example.json').read_text())
BASE={
    'name':'check', 'kind':'live_e2e', 'profiles':['raw','full'],
    'cn_counts':[2], 'workers_per_cn':[1], 'locks':[1],
    'distributions':['uniform'], 'probes':[0], 'work_ns':[0],
    'warmup_ms':100, 'measure_ms':500, 'paxos_slots':16384, 'max_ops':6000,
    'repetitions':2,
}
LIVE_LINE=('LIVE_E2E status=PASS scope=live_steady_state workers=2 locks=1 zipf=0 '
           'warmup_ms=100 measure_ms=500 completed=412 measured=307 joins=412 '
           'verified_markers=412 violations=0 join_aggregate=1 phase2_window=1 '
           'prepare_range=1 lookahead=2 replenish_threshold=1 batch_wait_us=0 '
           'join_batches=412 actual_join_batch_mean=1 prepare_records=412 '
           'latency_p50_ns=1000 latency_p95_ns=5000 latency_p99_ns=9000 '
           'admission_p50_ns=400 admission_p99_ns=1000 handoff_wait_p99_ns=2000 '
           'goodput_ops_per_sec=614 probe=1 work_ns=0')


class LiveSuiteTests(unittest.TestCase):
    def test_profiles_are_supported_and_independent(self):
        for name in PROFILES:
            p=profile(name)
            self.assertLessEqual(p['prepare_range'],p['lookahead'])
            self.assertLessEqual(p['replenish_threshold'],p['lookahead'])
        expected={'JOIN aggregation (raw context)':{'join_aggregate'},
                  'RDMA phase2 pipelining':{'phase2_window'},
                  'PREPARE range compression':{'prepare_range'},
                  'Prepared window depth':{'lookahead'},
                  'Proactive replenishment':{'replenish_threshold'},
                  'Prepared depth (full context)':{'lookahead'},
                  'Proactive trigger (full context)':{'replenish_threshold'},
                  'PREPARE compression (full context)':{'prepare_range'},
                  'RDMA pipeline (full context)':{'phase2_window'},
                  'JOIN aggregation (full context)':{'join_aggregate'}}
        for effect,control,treatment in ABLATIONS:
            diff={k for k in profile(control) if profile(control)[k]!=profile(treatment)[k]}
            self.assertTrue(diff, effect)
            if effect in expected:self.assertEqual(diff,expected[effect],effect)

    def test_live_suite_expansion_is_paired_and_deterministic(self):
        spec={'tests':[BASE], 'seed':2026, 'shuffle':True}
        cases=runner.expand_suite(spec,TOPO)
        self.assertEqual(len(cases),4)
        self.assertEqual(cases,runner.expand_suite(spec,TOPO))
        self.assertEqual(len(set(x['id'] for x in cases)),4)
        self.assertEqual({(x['profile'],x['trial']) for x in cases},
                         {('raw',1),('raw',2),('full',1),('full',2)})
        cfg=runner.case_topology(TOPO,cases[0],7777)
        self.assertEqual(cfg['benchmark']['mode'],'live')
        self.assertEqual(cfg['benchmark']['ballot'],7777)
        self.assertEqual(cfg['benchmark']['measure_ms'],500)
        self.assertEqual(cfg['clients'][0]['workers'],1)
        self.assertEqual(cfg['benchmark']['phase2_window'],profile(cases[0]['profile'])['phase2_window'])
        self.assertEqual(cluster.validate(cfg),2)

    def test_live_commands_do_not_put_clients_on_mns(self):
        case=runner.expand_suite({'tests':[BASE]},TOPO)[0]
        cfg=runner.case_topology(TOPO,case,4101)
        coordinator=cluster.commands(cfg,'coordinator')[0]
        self.assertEqual(coordinator[1],'coordinator')
        self.assertIn('mucql_lock_live',coordinator[0])
        self.assertIn('10.10.1.1:39660',coordinator)
        self.assertEqual(coordinator[-1],str(case['seed']))
        self.assertEqual(cluster.commands(cfg,'mn4')[0][1],'server')
        self.assertEqual(cluster.commands(cfg,'mn4')[0][-1],str(16384+2+32))
        self.assertIn('mucql_lock_live',cluster.commands(cfg,'cn1')[0][0])
        self.assertEqual(cluster.commands(cfg,'cn1')[0][4],'10.10.1.5')
        self.assertEqual(cluster.commands(cfg,'cn2')[0][4],'10.10.1.6')
        self.assertEqual(len(cluster.commands(cfg,'mn1')),1)

    def test_live_result_parse_rejects_unsafe_or_missing(self):
        data=runner.parse_summary('live_e2e',LIVE_LINE)
        self.assertEqual(data['measured'],'307')
        self.assertEqual(data['phase2_window'],'1')
        for bad in [LIVE_LINE.replace('violations=0','violations=1'),
                    LIVE_LINE.replace('verified_markers=412','verified_markers=411'),
                    LIVE_LINE.replace('joins=412','joins=413'),
                    LIVE_LINE.replace('goodput_ops_per_sec=614','goodput_ops_per_sec=nan'),
                    LIVE_LINE.replace('scope=live_steady_state','scope=precommitted_handoff'),
                    LIVE_LINE+'\n'+LIVE_LINE]:
            with self.assertRaises(ValueError):runner.parse_summary('live_e2e',bad)

    def test_nonmatching_settings_rejected(self):
        case=runner.expand_suite({'tests':[BASE]},TOPO)[0]
        doc=runner.case_topology(TOPO,case,4101)
        doc['benchmark']['prepare_range']=65
        with self.assertRaises(ValueError):cluster.validate(doc)
        doc['benchmark']['prepare_range']=1
        doc['benchmark']['replenish_threshold']=70
        with self.assertRaises(ValueError):cluster.validate(doc)

    def test_diagnostic_suite_matrix_and_matched_pairs(self):
        spec=json.loads((HERE/'suites/live_diagnostic.json').read_text())
        cases=runner.expand_suite(spec,TOPO)
        self.assertEqual(len(cases),28)
        self.assertEqual({c['cn_count']*c['workers_per_cn'] for c in cases},{2,8})
        self.assertEqual({c['profile'] for c in cases},{
            'raw','lookahead','replenish_control','replenish_proactive',
            'full_no_join','full_no_phase2','full'})
        self.assertTrue(all(c['paxos_slots']==131072 and c['max_ops']==90000 for c in cases))
        for workers in (2,8):
            peers=[c for c in cases if c['cn_count']*c['workers_per_cn']==workers]
            self.assertEqual({(c['profile'],c['trial']) for c in peers},
                             {(p,i) for p in {
                                 'raw','lookahead','replenish_control','replenish_proactive',
                                 'full_no_join','full_no_phase2','full'} for i in (1,2)})

    def test_failed_row_records_diagnostics_and_full_live_profile(self):
        c=runner.expand_suite({'tests':[BASE]},TOPO)[0]
        row=runner.failure_row(c,'a'*40,RuntimeError('RDMA registration failed'))
        self.assertEqual(row['status'],'FAIL')
        self.assertEqual(row['worker_processes'],2)
        self.assertEqual(row['cn_nodes'],2)
        self.assertEqual(row['paxos_slots'],16384)
        self.assertEqual(row['join_aggregate'],1)
        self.assertIn('RDMA registration failed',row['error'])
        self.assertIn('error',runner.COLUMNS)

    def test_run_matrix_bounds(self):
        spec=json.loads((HERE/'suites/live_ablations.json').read_text())
        cases=runner.expand_suite(spec,TOPO)
        self.assertEqual(len(cases),320)
        self.assertTrue(all(c['kind']=='live_e2e' for c in cases))
        spec2=json.loads((HERE/'suites/live_scaling.json').read_text())
        runner.expand_suite(spec2,TOPO)
        bad=dict(BASE,max_ops=16384)
        with self.assertRaisesRegex(ValueError,'headroom'):
            runner.expand_suite({'tests':[bad]},TOPO)
        bad=dict(BASE,profiles=['decLock'])
        with self.assertRaises(ValueError):runner.expand_suite({'tests':[bad]},TOPO)

    def test_paired_ablation_reports_improvement_and_failure(self):
        r=dict(status='PASS',kind='live_e2e',scenario='s',cn_nodes=2,worker_processes=8,
               lock_count=1,distribution='uniform',work_ns=0,probe=0,warmup_ms=200,
               measure_ms=1000,seed=2026)
        rows=[]
        for trial in range(1,6):
            rows.append(dict(r,profile='raw',trial=trial,goodput_ops_per_sec=str(1000+trial)))
            rows.append(dict(r,profile='full',trial=trial,goodput_ops_per_sec=str(1500+1.5*trial)))
        with tempfile.TemporaryDirectory() as d:
            out=Path(d)/'ablation.csv'
            runner.write_ablations(rows,out)
            with out.open() as f:
                data=list(csv.DictReader(f))
            match=next(x for x in data if x['effect']=='Combined optimization vs raw')
            self.assertEqual(match['matched_trials'],'5')
            self.assertEqual(match['median_speedup'],'1.5000')
            self.assertEqual(match['median_gain_pct'],'50.00')
            self.assertEqual(match['status'],'PASS')
            self.assertTrue(match['bootstrap95_low'])
            rows[-1]['status']='FAIL'
            runner.write_ablations(rows,out)
            with out.open() as f:
                match=next(x for x in csv.DictReader(f) if x['effect']=='Combined optimization vs raw')
            self.assertEqual(match['matched_trials'],'4')
            self.assertEqual(match['status'],'INCOMPLETE')

    def test_reject_fake_declock(self):
        with self.assertRaisesRegex(ValueError,'NOT implemented'):
            runner.expand_suite({'tests':[dict(BASE,kind='declock')]},TOPO)


if __name__=='__main__':unittest.main()

class LiveControllerLifecycleTests(unittest.TestCase):
    """An in-memory remote-agent stub exercises the actual controller sequence.

    No local NIC or SSH credentials are needed, so these are contract tests,
    not substitutes for real distributed hardware validation.
    """
    class FakeRemote:
        def __init__(self, primary):
            self.primary=primary
            self.events=[]
            self.synced={}
            self.started=[]
            self.stopped=[]

        def sync_config(self, node, cfg, json_text):
            self.events.append(('config',node['id']))
            self.synced[node['id']]=json.loads(json_text)

        def agent(self,node,*,cfg,job,role,action,timeout=20):
            self.events.append((action,role))
            if action=='start':
                self.started.append((node['id'],role,job))
                return {'started':True}
            if action=='status':
                return {'ready':True,'alive':True,'finished':True,
                        'returncode':0,'log_tail':''}
            if action=='probe':
                assert node['id'].startswith('cn')
                return {'probe':'PASS'}
            if action=='logs':
                return {'files':{'stdout.log': self.primary if role=='coordinator' else 'OK\n'}}
            if action=='stop':
                self.stopped.append((node['id'],role,job))
                return {'stopped':True}
            raise AssertionError(action)

    def _case(self):
        return runner.expand_suite({'tests':[dict(BASE, profiles=['raw'],
                        probes=[1], repetitions=1)]},TOPO)[0]

    def test_full_lifecycle_and_no_cn_on_mn(self):
        fake=self.FakeRemote(LIVE_LINE)
        case=self._case()
        with tempfile.TemporaryDirectory() as tmp:
            row=runner.benchmark(fake,case,TOPO,'c'*40,'run-a',Path(tmp),7777,30)
            self.assertEqual(row['kind'],'live_e2e')
            self.assertEqual(row['profile'],'raw')
            self.assertEqual(row['status'],'PASS')
            self.assertEqual(row['comparable_to_declock'],'NO')
            self.assertEqual(len(fake.synced),6)
            self.assertEqual(len(fake.started),7)
            self.assertEqual(set(x[1] for x in fake.started),
                  {'mn1','mn2','mn3','mn4','coordinator','cn1','cn2'})
            self.assertEqual(len(fake.stopped),len(fake.started))
            self.assertEqual(fake.events.index(('probe','cn1')) <
                             fake.events.index(('start','coordinator')),True)
            for node,role,_ in fake.started:
                if role.startswith('cn'):
                    self.assertEqual(node,role)
            self.assertTrue((Path(tmp)/case['id']/'coordinator'/'stdout.log').exists())

    def test_cleanup_failure_rejects_otherwise_passing_trial(self):
        class FailedCleanupRemote(self.FakeRemote):
            def agent(self,node,*,cfg,job,role,action,timeout=20):
                if action=='stop' and role=='mn4':
                    raise RuntimeError('TCP listener still on port 39670')
                return super().agent(node,cfg=cfg,job=job,role=role,
                                     action=action,timeout=timeout)
        fake=FailedCleanupRemote(LIVE_LINE)
        case=self._case()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError,'cleanup incomplete'):
                runner.benchmark(fake,case,TOPO,'c'*40,'run-stop-error',
                                 Path(tmp),7777,30)
            self.assertEqual(len(fake.stopped),6)
            self.assertIn('39670',(Path(tmp)/case['id']/'cleanup_error_mn4.txt').read_text())

    def test_invalid_result_still_cleans_every_job(self):
        bad=LIVE_LINE.replace('verified_markers=412','verified_markers=410')
        fake=self.FakeRemote(bad)
        case=self._case()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError,'marker'):
                runner.benchmark(fake,case,TOPO,'c'*40,'run-b',Path(tmp),7777,30)
            self.assertEqual(len(fake.started),7)
            self.assertEqual(len(fake.stopped),7)