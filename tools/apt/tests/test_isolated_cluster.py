import json
import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import isolated_cluster as ic

class TopologyTests(unittest.TestCase):
    def setUp(self):
        self.doc = json.loads((Path(__file__).resolve().parents[1] / 'topology.example.json').read_text())

    def test_worker_ids_and_no_client_on_mn(self):
        self.assertEqual(ic.validate(self.doc), 8)
        self.assertEqual(ic.workers_for(self.doc, 'cn1'), [0,1,2,3])
        self.assertEqual(ic.workers_for(self.doc, 'cn2'), [4,5,6,7])
        self.assertEqual(len(ic.commands(self.doc, 'cn2')),4)
        self.assertEqual(ic.commands(self.doc,'cn2')[0][3], '4')
        self.assertIn('10.10.1.4:29670', ic.commands(self.doc,'coordinator')[0])
        self.assertIn('10.10.1.3:29660', ic.commands(self.doc,'coordinator')[0])

    def test_reject_colocation(self):
        self.doc['clients'][0]['ip'] = '10.10.1.1'
        with self.assertRaisesRegex(ValueError,'physically distinct'):
            ic.validate(self.doc)

    def test_reject_role_clash(self):
        self.doc['clients'][0]['id'] = 'mn1'
        with self.assertRaises(ValueError):
            ic.validate(self.doc)

    def test_reject_oversubscribe(self):
        self.doc['clients'][0]['workers'] = 9
        with self.assertRaisesRegex(ValueError,'oversubscribing'):
            ic.validate(self.doc)

    def test_reject_capacity(self):
        for j in range(5):
            self.doc['clients'].append({'id':f'cn{j+3}','ip':f'10.10.1.{j+7}','workers':16,'cores':16})
        with self.assertRaisesRegex(ValueError,'64'):
            ic.validate(self.doc)

    def test_reject_positions(self):
        self.doc['benchmark']['positions']=7
        with self.assertRaisesRegex(ValueError,'positions'):
            ic.validate(self.doc)

    def test_reject_batch(self):
        self.doc['benchmark']['order_batch']=33
        with self.assertRaisesRegex(ValueError,'order_batch'):
            ic.validate(self.doc)

    def test_override_in_commands(self):
        c=ic.commands(self.doc,'coordinator',ballot=5107,positions=2400,batch=32,probe=0)[0]
        self.assertEqual(c[3],'5107')
        self.assertEqual(c[4],'2400')
        self.assertEqual(c[-2],'0')
        t=ic.commands(self.doc,'ordering',batch=32)[0]
        self.assertEqual(t[-1],'32')
        self.assertEqual(t[1],'client')

if __name__ == '__main__':
    unittest.main()
