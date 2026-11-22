"""Offline DecLock experiment planning and goodput validation tests."""
import json
from pathlib import Path
import sys
import unittest

HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
import declock_match_plan as plan
import parse_declock_goodput as parse


class DecLockPlanTests(unittest.TestCase):
    def setUp(self):
        self.doc=json.loads((HERE/'topology.example.json').read_text())

    def test_command_contract_upstream_exclusive_shot_loop(self):
        a=plan.command('dmlock',8,1,32)
        self.assertEqual(a,[
            'bash','scripts/run-on-all.sh','dmlock_microbench',
            '0.99','0','2','8','1','1','0','8','32','1','0','1'])
        b=plan.command('dmlock_tf',4,1000,1)
        self.assertEqual(b[2],'dmlock_tf_microbench')
        self.assertEqual(b[6],'4')
        self.assertEqual(b[8],'1000')
        self.assertEqual(b[11],'1')
        for system,w,locks,shots in [
            ('raw',8,1,1),('dmlock',0,1,1),('dmlock',8,0,1),
            ('dmlock',8,1,0),('dmlock',8,1,33),('dmlock',1.5,1,1)]:
            with self.assertRaises(ValueError):
                plan.command(system,w,locks,shots)

    def test_build_manifest_has_no_execution_and_matched_worker_counts(self):
        cases=plan.build_plan(self.doc,Path('/tmp/declock'),
                              ['dmlock','dmlock_tf'],[4,8],[1],[1,32],5)
        self.assertEqual(len(cases),40)
        self.assertEqual({x['active_clients'] for x in cases},{8,16})
        self.assertEqual({x['test_time_ms'] for x in cases},{2000})
        self.assertEqual({x['measurement_status'] for x in cases},{'NOT_RUN'})
        self.assertEqual({x['read_ratio'] for x in cases},{0})
        self.assertEqual({x['data_size_bytes'] for x in cases},{8})
        self.assertEqual({x['data_ops'] for x in cases},{1,32})
        self.assertEqual({x['artifact_commit'] for x in cases},{plan.PINNED_DECLOCK_SHA})
        self.assertTrue(all(c['argv'][7]=='1' for c in cases))
        self.assertTrue(all(c['argv'][12]=='1' for c in cases))
        with self.assertRaisesRegex(ValueError,'core budget'):
            plan.build_plan(self.doc,Path('/tmp/declock'),['dmlock'],[16],[1],[1],1)

    def test_host_mapping_preserves_separate_mn_cn_roles(self):
        env=plan.host_environment(self.doc,Path('/tmp/declock'),'tester')
        self.assertIn('10.10.1.4-10.10.1.4-0',env)
        self.assertIn('10.10.1.5-10.10.1.5-0',env)
        self.assertIn('10.10.1.6-10.10.1.6-0',env)
        self.assertNotIn('10.10.1.1-',env)
        self.assertIn('USERNAME=tester',env)
        with self.assertRaises(ValueError):
            plan.host_environment(self.doc,Path('/tmp/declock'),'some user;rm -rf')


class DecLockGoodputTests(unittest.TestCase):
    def sample(self,host,rate):
        return ''.join(
            f'[host{host}][core{i}] request throughput 9.0000 Mops\n'
            f'[host{host}][core{i}] completed lock cycles 200\n'
            f'[host{host}][core{i}] successful lock throughput {rate:.4f} Mops\n'
            for i in range(2))

    def test_aggregate_success_and_never_attempts(self):
        result=parse.parse([self.sample(1,0.0001),self.sample(2,0.0002)],2)
        self.assertEqual(result['workers'],4)
        self.assertEqual(result['completed_cycles'],800)
        self.assertEqual(result['goodput_ops_per_sec'],600)
        self.assertFalse(result['request_attempts_used'])
        self.assertNotEqual(result['goodput_ops_per_sec'],36_000_000)

    def test_combined_stderr_log_with_info_prefix(self):
        stderr='[Info]'+self.sample(1,0.0001).replace('[host','[Info][host')
        stderr+=self.sample(2,0.0002).replace('[host','[Info][host')
        result=parse.parse([stderr],2)
        self.assertEqual(result['workers'],4)
        self.assertEqual(result['goodput_ops_per_sec'],600)
        self.assertEqual(result['status'],'PARSED_ONLY')

    def test_reject_unpatched_duplicate_partial_and_invalid_counts(self):
        a=self.sample(1,0.0001)
        b=self.sample(2,0.0001)
        for x,y in [
            (a.replace('successful lock throughput','request throughput'),b),
            (a,a),
            (a.replace('completed lock cycles 200','completed lock cycles 0'),b),
            (a.replace('successful lock throughput 0.0001','successful lock throughput nan'),b),
            (a,b.replace('[core1]','[core0]')),
        ]:
            with self.assertRaises(ValueError):
                parse.parse([x,y],2)


if __name__=='__main__':
    unittest.main()
