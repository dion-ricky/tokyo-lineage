import unittest
import subprocess

class TestInitAirflow(unittest.TestCase):

    def test_init_airflow(self):
        command = "airflow initdb"
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        process.wait()

        self.assertEqual(process.returncode, 0)

if __name__ == '__main__':
    unittest.main()