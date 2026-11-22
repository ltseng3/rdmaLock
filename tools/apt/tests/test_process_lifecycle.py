"""Real Linux process tree and TCP listener regression tests; no RNIC needed."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import process_lifecycle as life
import isolated_cluster as cluster
TOPO=json.loads((Path(__file__).resolve().parents[1]/'topology.example.json').read_text())


class LifecycleTests(unittest.TestCase):
    def test_role_ports_are_per_host(self):
        doc=dict(TOPO)
        doc['ports']={'ordering':39660,'data':39670,'coordinator':39700,'worker_base':39711}
        self.assertEqual(life.role_ports(doc,'mn1',cluster),[39660])
        self.assertEqual(life.role_ports(doc,'mn4',cluster),[39670])
        self.assertEqual(life.role_ports(doc,'coordinator',cluster),[39700])
        self.assertEqual(life.role_ports(doc,'ordering',cluster),[])
        self.assertEqual(life.role_ports(doc,'cn1',cluster),[39711,39712,39713,39714])
        self.assertEqual(life.role_ports(doc,'cn2',cluster),[39715,39716,39717,39718])

    def test_port_check_ignores_bound_nonlistener_and_does_not_kill_foreign(self):
        s=socket.socket()
        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        s.bind(('127.0.0.1',0))
        port=s.getsockname()[1]
        try:
            self.assertNotIn(port,life.listeners())
            s.listen(1)
            self.assertIn(port,life.listeners())
            with self.assertRaisesRegex(RuntimeError,'Refusing to terminate'):
                life.wait_ports_free([port],timeout=.01)
        finally:
            s.close()
        life.wait_ports_free([port],timeout=1)

    def test_bind_probe_catches_port_occupied_without_listen(self):
        # /proc/net/tcp has no LISTEN entry for this socket, but a new
        # wildcard server cannot bind. This is precisely what the old
        # LISTEN-only teardown check incorrectly accepted.
        s=socket.socket()
        s.bind(('0.0.0.0',0))
        port=s.getsockname()[1]
        try:
            self.assertNotIn(port,life.listeners())
            self.assertFalse(life.can_bind_and_listen(port))
            with self.assertRaisesRegex(RuntimeError,'bind/listen port'):
                life.wait_ports_free([port],timeout=.03)
        finally:
            s.close()
        self.assertTrue(life.can_bind_and_listen(port))
        life.wait_ports_free([port],timeout=1)

    def test_retry_probe_waits_for_stopping_socket(self):
        s=socket.socket()
        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        s.bind(('0.0.0.0',0))
        port=s.getsockname()[1]
        s.listen(1)
        thread=threading.Thread(target=lambda:(time.sleep(.12),s.close()))
        thread.start()
        try:
            self.assertFalse(life.can_bind_and_listen(port))
            life.wait_ports_free([port],timeout=2)
        finally:
            thread.join(timeout=1)
            s.close()

    def test_port_probe_rejects_invalid_ports(self):
        for port in (0,-1,65536,1.5,'39670',None):
            with self.assertRaises(ValueError):
                life.can_bind_and_listen(port)

    def test_24_repeated_restart_cycles_release_listener(self):
        with tempfile.TemporaryDirectory() as d:
            for trial in range(24):
                f=Path(d)/f'port-{trial}'
                script=('import pathlib,socket,time;'
                        's=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);'
                        's.bind(("127.0.0.1",0));s.listen(3);'
                        f'pathlib.Path({str(f)!r}).write_text(str(s.getsockname()[1]));'
                        'time.sleep(30)')
                tag=f'restart-test-{trial}'
                env=dict(os.environ, **{life.JOB_ENV:tag})
                proc=subprocess.Popen([sys.executable,'-c',script],env=env,start_new_session=True)
                try:
                    deadline=time.monotonic()+2
                    port_text=''
                    while time.monotonic()<deadline:
                        if f.exists():
                            port_text=f.read_text().strip()
                            if port_text:
                                break
                        time.sleep(.01)
                    self.assertTrue(port_text, 'child did not publish a TCP listening port')
                    port=int(port_text)
                    self.assertIn(port,life.listeners())
                    self.assertTrue(life.owned_group_members(proc.pid,tag))
                    life.stop_owned_group(proc.pid,tag,grace=.5,kill_grace=.5)
                    life.wait_ports_free([port],timeout=1)
                finally:
                    try:os.killpg(proc.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    proc.wait(timeout=1)

    def test_orphan_child_holds_port_and_is_cleaned_without_killing_foreign(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'child.pid'
            portfile=Path(d)/'port'
            child=('import socket,signal,time,pathlib;'
                   'signal.signal(signal.SIGTERM,signal.SIG_IGN);'
                   's=socket.socket();s.bind(("127.0.0.1",0));s.listen(2);'
                   f'pathlib.Path({str(portfile)!r}).write_text(str(s.getsockname()[1]));'
                   'time.sleep(30)')
            parent=('import subprocess,sys,pathlib,time;'
                    f'c=subprocess.Popen([sys.executable,"-c",{child!r}]);'
                    f'pathlib.Path({str(path)!r}).write_text(str(c.pid));'
                    f'p=pathlib.Path({str(portfile)!r});'
                    '[(time.sleep(.02)) for _ in range(100) if not p.exists()]')
            env=dict(os.environ, **{life.JOB_ENV:'test-orphan-123'})
            proc=subprocess.Popen([sys.executable,'-c',parent],env=env,start_new_session=True)
            try:
                proc.wait(timeout=5)
                child_pid=int(path.read_text())
                deadline=time.monotonic()+2
                port_text=''
                while time.monotonic()<deadline:
                    if portfile.exists():
                        port_text=portfile.read_text().strip()
                        if port_text:
                            break
                    time.sleep(.01)
                self.assertTrue(port_text, 'orphan did not publish a TCP listening port')
                port=int(port_text)
                self.assertIn(port,life.listeners())
                self.assertIn(child_pid,life.owned_group_members(proc.pid,'test-orphan-123'))
                self.assertEqual(life.owned_group_members(proc.pid,'different-job'),[])
                r=life.stop_owned_group(proc.pid,'test-orphan-123',grace=.1,kill_grace=1)
                self.assertTrue(r['escalated'])
                life.wait_ports_free([port],timeout=2)
            finally:
                try:os.killpg(proc.pid,signal.SIGKILL)
                except ProcessLookupError:pass


if __name__=='__main__':unittest.main()
