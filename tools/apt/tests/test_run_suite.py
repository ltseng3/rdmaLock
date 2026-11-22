import json
import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run_suite as auto
import node_agent as agent

TOPO = json.loads((Path(__file__).resolve().parents[1] / 'topology.example.json').read_text())

class SuiteTests(unittest.TestCase):
    def test_smoke_expands_and_preserves_labels(self):
        spec=json.loads((Path(__file__).resolve().parents[1] / 'suites/smoke.json').read_text())
        cases=auto.expand_suite(spec, TOPO)
        self.assertEqual(len(cases),7)
        self.assertEqual(sum(c['kind']=='ordering_micro' for c in cases),3)
        self.assertEqual(sum(c['kind']=='prepared_e2e' for c in cases),4)
        self.assertEqual(len(set(c['id'] for c in cases)),len(cases))

    def test_ablations_expand(self):
        spec=json.loads((Path(__file__).resolve().parents[1] / 'suites/ablations.json').read_text())
        cases=auto.expand_suite(spec, TOPO)
        self.assertTrue(len(cases)>40)
        self.assertTrue(all(c['positions']<=3000 for c in cases))

    def test_reject_unsupported_and_fabricated_comparison(self):
        for k in ['live', 'declock', 'raw', 'steady_state']:
            with self.assertRaisesRegex(ValueError,'NOT implemented'):
                auto.expand_suite({'tests':[{'name':'invalid','kind':k}]},TOPO)

    def test_reject_oversubscription(self):
        with self.assertRaisesRegex(ValueError,'oversubscribe'):
            auto.expand_suite({'tests':[{'name':'bad','kind':'prepared_e2e','workers_per_cn':[9]}]},TOPO)

    def test_topology_reduction_and_count(self):
        c={'kind':'prepared_e2e','cn_count':2,'workers_per_cn':2,'positions':512,
           'batch':16,'probe':1,'work_ns':0}
        d=auto.case_topology(TOPO,c,5101)
        self.assertEqual(sum(x['workers'] for x in d['clients']),4)
        self.assertEqual(d['benchmark']['ballot'],5101)
        self.assertEqual(d['benchmark']['order_batch'],16)

    def test_e2e_checks_parse(self):
        text='LOCK_E2E positions=12 workers=3 completed=12 violations=0 verified_markers=12 ordered_entries=13 probe=1 work_ns=0 ordering_seconds=1 handoff_seconds=1 goodput_ops_per_sec=12 including_ordering_ops_per_sec=6 status=PASS'
        self.assertEqual(auto.parse_summary('prepared_e2e',text)['completed'],'12')
        with self.assertRaises(ValueError):
            auto.parse_summary('prepared_e2e',text.replace('verified_markers=12','verified_markers=11'))
        with self.assertRaises(ValueError):
            auto.parse_summary('prepared_e2e',text.replace('goodput_ops_per_sec=12','goodput_ops_per_sec=nan'))

    def test_ordering_checks_parse(self):
        text='One-sided RDMA phase2 committed=1000 batch=16 seconds=0.2 ordered_entries_per_sec=5000'
        self.assertEqual(auto.parse_summary('ordering_micro',text)['batch'],'16')
        with self.assertRaisesRegex(ValueError,'exactly one'):
            auto.parse_summary('ordering_micro',text+'\n'+text)

    def test_median_csv_separate_metric_families(self):
        with tempfile.TemporaryDirectory() as d:
            o=Path(d)/'summary.csv'
            auto.summarize([
                {'status':'PASS','kind':'ordering_micro','cn_nodes':0,'worker_processes':0,'positions':512,'batch':8,'probe':0,'work_ns':0,'ordered_entries_per_sec':'100'},
                {'status':'PASS','kind':'ordering_micro','cn_nodes':0,'worker_processes':0,'positions':512,'batch':8,'probe':0,'work_ns':0,'ordered_entries_per_sec':'200'},
                {'status':'FAIL','kind':'ordering_micro','cn_nodes':0,'worker_processes':0,'positions':512,'batch':8,'probe':0,'work_ns':0,'ordered_entries_per_sec':'10000'}],o)
            self.assertIn('150.00',o.read_text())
            self.assertNotIn('10000',o.read_text())

    def test_job_path_traversal_rejected(self):
        for value in ('', '../bad','bad/abc','bad\\abc'):
            with self.assertRaises(ValueError):
                agent.location(value)

    def test_agent_process_status_for_finished(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)
            (p/'state.json').write_text(json.dumps({'role':'coordinator','ready_token':'lock coordinator listening tcp='}))
            (p/'exit.json').write_text(json.dumps({'returncode':0}))
            (p/'stdout.log').write_text('lock coordinator listening tcp=29700\n')
            s=agent.snapshot(p)
            self.assertEqual(s['returncode'],0)
            self.assertFalse(s['alive'])

    def test_bootstrap_orders_dependency_check_before_cmake_build(self):
        ex=auto.Executor(username='tester',checkout=Path('/tmp'),local_ips=set())
        calls=[]
        ex.shell=lambda node, script, **kw: (calls.append((script,kw)) or 'OK')
        self.assertEqual(ex.bootstrap({'id':'cn1','ip':'10.10.1.5'}, 'a'*40, 'mlx4_0', 1), 'OK')
        script, opts = calls[0]
        self.assertLess(script.index('git clone'), script.index('ensure_deps.sh auto'))
        self.assertLess(script.index('ensure_deps.sh auto'), script.index('tools/apt/build.sh'))
        self.assertLess(script.index('tools/apt/build.sh'), script.index('tools/apt/check.sh'))
        self.assertEqual(opts['timeout'], 900)

    def test_bootstrap_explicit_check_only_never_installs(self):
        ex=auto.Executor(username='tester',checkout=Path('/tmp'),local_ips=set())
        calls=[]
        ex.shell=lambda node, script, **kw: (calls.append((script,kw)) or 'OK')
        ex.bootstrap({'id':'cn1','ip':'10.10.1.5'}, 'a'*40, 'mlx4_0', 1,
                     auto_deps=False)
        script, opts = calls[0]
        self.assertIn('tools/apt/ensure_deps.sh check', script)
        self.assertNotIn('setup.sh', script)
        self.assertEqual(opts['timeout'], 360)

    def test_bootstrap_full_setup_still_checks_dependencies(self):
        ex=auto.Executor(username='tester',checkout=Path('/tmp'),local_ips=set())
        calls=[]
        ex.shell=lambda node, script, **kw: (calls.append((script,kw)) or 'OK')
        ex.bootstrap({'id':'cn1','ip':'10.10.1.5'}, 'a'*40, 'mlx4_0', 1,
                     install_deps=True)
        script, _ = calls[0]
        self.assertLess(script.index('tools/apt/setup.sh'), script.index('ensure_deps.sh auto'))
        self.assertLess(script.index('ensure_deps.sh auto'), script.index('tools/apt/build.sh'))

    def test_restart_stress_expansion_and_port_budget(self):
        spec=json.loads((Path(__file__).resolve().parents[1] /
                         'suites/live_restart_stress.json').read_text())
        cases=auto.expand_suite(spec, TOPO)
        self.assertEqual(len(cases),24)
        self.assertEqual({(c['profile'],c['trial']) for c in cases},
                         {(p,i) for p in ('raw','full') for i in range(1,13)})
        self.assertTrue(all(c['paxos_slots']==131072 and c['max_ops']==90000 for c in cases))

    def test_lifecycle_agent_has_a_job_tag(self):
        self.assertEqual(__import__('process_lifecycle').JOB_ENV,'MUCQL_AUTOMATION_JOB')

    def test_transport_isolation_env_set_before_live_launch(self):
        source=(Path(__file__).resolve().parents[1]/'isolated_cluster.py').read_text()
        self.assertIn('env["MUCQL_CN_TRANSPORT"] =',source)

    def test_controller_ssh_exec_is_noninteractive(self):
        ex=auto.Executor(username='tsengle',checkout=Path('/tmp'),local_ips=set())
        self.assertEqual(ex.is_local({'ip':'10.10.1.1'}),False)
        self.assertIn('BatchMode=yes',auto.SSH_OPTS)

if __name__=='__main__':
    unittest.main()
