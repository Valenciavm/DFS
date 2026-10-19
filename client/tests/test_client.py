import unittest
from unittest.mock import Mock
from client import DFShaClient, MFARequired

class ClientTests(unittest.TestCase):
    def setUp(self):
        self.c=DFShaClient('http://nn.test:8000'); self.c.http.request=Mock()
    def test_mfa_login(self):
        self.c.http.request.return_value={'mfa_required':True,'challenge':'mfa_test','expires_in':300}
        with self.assertRaises(MFARequired) as err: self.c.login('ana','secret')
        self.assertEqual(err.exception.challenge,'mfa_test')
    def test_relative_path(self):
        self.c.current_dir='/projects'; self.assertEqual(self.c.resolve_path('a.txt'),'/projects/a.txt')
    def test_ls_pagination(self):
        self.c.http.request.side_effect=[{'entries':[{'name':'a'}],'next_cursor':'x'},{'entries':[{'name':'b'}],'next_cursor':None}]
        self.assertEqual([x['name'] for x in self.c.ls('/')],['a','b'])
    def test_invalid_acl(self):
        with self.assertRaises(ValueError): self.c.set_acl('/x','user:a','bad')
if __name__=='__main__': unittest.main()
