"""Hermetic dependency bootstrap tests: never invoke the host's apt or sudo."""
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'ensure_deps.sh'


class BootstrapDepsTests(unittest.TestCase):
    def fake_path(self, root, *, has_cmake, has_apt=True, apt_fails=False):
        bindir = Path(root)
        for cmd in ('bash', 'env', 'sort'):
            (bindir / cmd).symlink_to('/bin/bash' if cmd == 'bash' else '/usr/bin/' + cmd)
        # The hermetic PATH keeps the real host's package manager unreachable.
        for cmd in ('c++', 'make', 'git', 'python3', 'pkg-config', 'ibv_devices',
                    'ibv_devinfo', 'ibstat', 'lspci', 'ip'):
            (bindir / cmd).symlink_to('/usr/bin/true')
        sudo = bindir / 'sudo'
        sudo.write_text('#!/bin/bash\n'
                        'if [[ "$1" == -n ]]; then shift; fi\n'
                        'if [[ "$1" == true ]]; then exit 0; fi\n'
                        'exec "$@"\n')
        sudo.chmod(0o755)
        if has_cmake:
            (bindir / 'cmake').symlink_to('/usr/bin/true')
        if has_apt:
            p = bindir / 'apt-get'
            p.write_text('#!/bin/bash\n'
                         'if [[ "$1" == update ]]; then exit 0; fi\n'
                         'if [[ "$1" == install ]]; then\n'
                         '  if [[ "${MOCK_APT_FAIL:-0}" == 1 ]]; then exit 17; fi\n'
                         '  printf "#!/bin/sh\\nexit 0\\n" > "$MOCK_BIN/cmake"\n'
                         '  /bin/chmod +x "$MOCK_BIN/cmake"\n'
                         '  exit 0\n'
                         'fi\nexit 90\n')
            p.chmod(0o755)
        return {'PATH': str(bindir), 'MOCK_BIN': str(bindir),
                'MOCK_APT_FAIL': '1' if apt_fails else '0'}

    def test_present_deps_no_apt_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = self.fake_path(tmp, has_cmake=True, has_apt=False)
            r = subprocess.run(['/bin/bash', str(SCRIPT), 'auto'], env=env,
                               text=True, capture_output=True)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn('nothing to install', r.stdout)

    def test_check_only_fails_actionably(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = self.fake_path(tmp, has_cmake=False, has_apt=True)
            r = subprocess.run(['/bin/bash', str(SCRIPT), 'check'], env=env,
                               text=True, capture_output=True)
            self.assertEqual(r.returncode, 2)
            self.assertIn('cmake', r.stdout)
            self.assertIn('Auto-install disabled', r.stderr)
            self.assertFalse((Path(tmp) / 'cmake').exists())

    def test_auto_installs_only_missing_package_then_rechecks(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = self.fake_path(tmp, has_cmake=False, has_apt=True)
            r = subprocess.run(['/bin/bash', str(SCRIPT), 'auto'], env=env,
                               text=True, capture_output=True)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn('Installing missing packages: cmake', r.stdout)
            self.assertIn('dependencies: OK', r.stdout)

    def test_apt_failure_is_fatal(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = self.fake_path(tmp, has_cmake=False, has_apt=True, apt_fails=True)
            r = subprocess.run(['/bin/bash', str(SCRIPT), 'auto'], env=env,
                               text=True, capture_output=True)
            self.assertNotEqual(r.returncode, 0)
            self.assertFalse((Path(tmp) / 'cmake').exists())

    def test_no_package_manager_is_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = self.fake_path(tmp, has_cmake=False, has_apt=False)
            r = subprocess.run(['/bin/bash', str(SCRIPT), 'auto'], env=env,
                               text=True, capture_output=True)
            self.assertEqual(r.returncode, 2)
            self.assertIn('apt-get unavailable', r.stderr)


if __name__ == '__main__':
    unittest.main()
