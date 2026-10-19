"""High-level DFSha Python client, following docs/API.md."""
import hashlib, os
from pathlib import Path
from urllib.parse import quote
from .http import NameNodeHTTP
from .exceptions import MFARequired, IntegrityError
from .transfer import GrpcBlockTransport

class DFShaClient:
    def __init__(self, namenode_urls, *, timeout=15, verify_tls=True, max_retries=2, block_transport=None, page_size=1000):
        self.http = NameNodeHTTP(namenode_urls, timeout, verify_tls, max_retries)
        self.block_transport = block_transport or GrpcBlockTransport()
        self.page_size, self.username, self.current_dir, self._mfa_challenge = page_size, None, '/', None
    @property
    def authenticated(self): return bool(self.http.token)
    def register(self, username, password):
        return self.http.request('POST','/v1/auth/register',json={'username':username,'password':password},authenticated=False)
    def login(self, username, password):
        r = self.http.request('POST','/v1/auth/login',json={'username':username,'password':password},authenticated=False)
        self.username = username
        if r.get('mfa_required'):
            self._mfa_challenge = r['challenge']; raise MFARequired(r['challenge'],r.get('expires_in'))
        self._set_auth(r); return r
    def login_2fa(self, challenge, code):
        r = self.http.request('POST','/v1/auth/login/2fa',json={'challenge':challenge,'code':code},authenticated=False)
        self._mfa_challenge = None; self._set_auth(r); return r
    def login_pending_2fa(self, code):
        if not self._mfa_challenge: raise ValueError('No pending MFA challenge; call login() first')
        return self.login_2fa(self._mfa_challenge,code)
    def _set_auth(self, r):
        if not r.get('access_token'): raise ValueError('NameNode response missing access_token')
        self.http.set_token(r['access_token'],r.get('token_type','Bearer'))
    def use_api_key(self, key):
        if not key or not key.startswith('dfsha_live_'): raise ValueError("API key must begin with 'dfsha_live_'")
        self.http.set_token(key); return self
    def logout(self): self.http.token=None; self.username=None; self._mfa_challenge=None
    def resolve_path(self, path):
        if not path: raise ValueError('Path cannot be empty')
        return os.path.normpath(path if path.startswith('/') else os.path.join(self.current_dir,path))
    def pwd(self): return self.current_dir
    def cd(self,path):
        p=self.resolve_path(path)
        if self.stat(p).get('type')!='dir': raise NotADirectoryError(p)
        self.current_dir=p; return p
    def ls(self,path='.',limit=None):
        p=self.resolve_path(path); cursor=None; entries=[]
        while True:
            params={'path':p,'limit':limit or self.page_size}
            if cursor: params['cursor']=cursor
            page=self.http.request('GET','/v1/fs/ls',params=params); entries.extend(page.get('entries',[]))
            cursor=page.get('next_cursor')
            if not cursor: return entries
    def stat(self,path): return self.http.request('GET','/v1/fs/stat',params={'path':self.resolve_path(path)})
    def mkdir(self,path,parents=False): return self.http.request('POST','/v1/fs/mkdir',json={'path':self.resolve_path(path),'parents':bool(parents)})
    def rmdir(self,path,recursive=False): return self.http.request('DELETE','/v1/fs/rmdir',params={'path':self.resolve_path(path),'recursive':str(bool(recursive)).lower()})
    def rm(self,path): return self.http.request('DELETE','/v1/fs/rm',params={'path':self.resolve_path(path)})
    def mv(self,src,dst): return self.http.request('POST','/v1/fs/mv',json={'src':self.resolve_path(src),'dst':self.resolve_path(dst)})
    def set_acl(self,path,subject,permission):
        if permission not in {'r','rw','none'}: raise ValueError("permission must be 'r', 'rw', or 'none'")
        return self.http.request('PUT','/v1/fs/acl',json={'path':self.resolve_path(path),'subject':subject,'permission':permission})
    def get_acl(self,path): return self.http.request('GET','/v1/fs/acl',params={'path':self.resolve_path(path)})
    def lock(self,path,mode='shared',ttl_s=60):
        if mode not in {'shared','exclusive'}: raise ValueError("mode must be 'shared' or 'exclusive'")
        return self.http.request('POST','/v1/fs/lock',json={'path':self.resolve_path(path),'mode':mode,'ttl_s':int(ttl_s)})
    def renew_lock(self,lock_id,ttl_s=60): return self.http.request('POST',f'/v1/fs/lock/{quote(lock_id,safe="")}/renew',json={'ttl_s':int(ttl_s)})
    def unlock(self,lock_id): return self.http.request('DELETE',f'/v1/fs/lock/{quote(lock_id,safe="")}')
    def create_group(self,name): return self.http.request('POST','/v1/groups',json={'name':name})
    def group_info(self,name): return self.http.request('GET',f'/v1/groups/{quote(name,safe="")}')
    def add_members(self,name,usernames,remove=None): return self.http.request('PUT',f'/v1/groups/{quote(name,safe="")}/members',json={'add':list(usernames),'remove':list(remove or [])})
    def create_api_key(self,name): return self.http.request('POST','/v1/auth/apikeys',json={'name':name})
    def list_api_keys(self): return self.http.request('GET','/v1/auth/apikeys')
    def revoke_api_key(self,key_id): return self.http.request('DELETE',f'/v1/auth/apikeys/{quote(key_id,safe="")}')
    def put_file(self,local_path,remote_path,overwrite=False):
        local=Path(local_path)
        if not local.is_file(): raise FileNotFoundError(local)
        remote=self.resolve_path(remote_path); size=local.stat().st_size
        reserve=self.http.request('POST','/v1/files/put',json={'path':remote,'size':size,'overwrite':bool(overwrite)})
        fid=reserve['file_id']; checks=[]; whole=hashlib.sha256()
        try:
            with local.open('rb') as f:
                for b in sorted(reserve.get('blocks',[]),key=lambda x:x['index']):
                    parts=[]; remaining=int(b['size'])
                    while remaining:
                        chunk=f.read(min(1024*1024,remaining))
                        if not chunk: raise IOError('Local file ended before reserved block size')
                        parts.append(chunk); remaining-=len(chunk)
                    data=b''.join(parts); checksum=hashlib.sha256(data).hexdigest(); whole.update(data)
                    pipe=b.get('pipeline') or []
                    if not pipe: raise IOError(f"No pipeline for block {b.get('block_id')}")
                    self.block_transport.write_block(addr=pipe[0]['addr'],token=b['token'],block_id=b['block_id'],data=data,checksum_sha256=checksum,pipeline=[n['internal_addr'] for n in pipe[1:]])
                    checks.append({'index':b['index'],'block_id':b['block_id'],'size':len(data),'checksum':checksum})
            return self.http.request('POST',f'/v1/files/{quote(fid,safe="")}/commit',json={'blocks':checks,'file_checksum':whole.hexdigest()})
        except Exception:
            try: self.http.request('POST',f'/v1/files/{quote(fid,safe="")}/abort',json={})
            except Exception: pass
            raise
    def get_file(self,remote_path,local_path):
        manifest=self.http.request('GET','/v1/files/get',params={'path':self.resolve_path(remote_path)})
        target=Path(local_path); target.parent.mkdir(parents=True,exist_ok=True); temp=target.with_name(target.name+'.dfsha-part'); whole=hashlib.sha256()
        try:
            with temp.open('wb') as out:
                for b in sorted(manifest.get('blocks',[]),key=lambda x:x['index']):
                    data=None; last=None
                    for replica in b.get('replicas',[]):
                        try:
                            payload,_=self.block_transport.read_block(addr=replica['addr'],token=b['token'],block_id=b['block_id'],offset=0,length=0)
                            if len(payload)!=int(b['size']) or hashlib.sha256(payload).hexdigest()!=b['checksum']: raise IntegrityError(f"Integrity check failed for {b['block_id']}")
                            data=payload; break
                        except Exception as e: last=e
                    if data is None: raise last or IOError(f"No live replicas for {b['block_id']}")
                    out.write(data); whole.update(data)
            digest=whole.hexdigest()
            if manifest.get('checksum') and manifest['checksum']!=digest: raise IntegrityError('Whole-file SHA-256 mismatch')
            os.replace(temp,target); return {'path':str(target),'size':target.stat().st_size,'checksum':digest}
        except Exception:
            try: temp.unlink(missing_ok=True)
            except OSError: pass
            raise
