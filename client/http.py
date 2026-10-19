"""NameNode REST transport with basic failover."""
import time, uuid
from urllib.parse import urljoin
import requests
from .exceptions import APIError, AuthenticationError

class NameNodeHTTP:
    def __init__(self, base_urls, timeout=15, verify_tls=True, max_retries=2):
        if isinstance(base_urls, str): base_urls = [u.strip() for u in base_urls.split(',') if u.strip()]
        self.base_urls = [u.rstrip('/') + '/' for u in base_urls]
        if not self.base_urls: raise ValueError('At least one NameNode URL is required')
        self.timeout, self.verify_tls, self.max_retries = timeout, verify_tls, max(0, max_retries)
        self.session, self.token, self.token_type = requests.Session(), None, 'Bearer'
    def set_token(self, token, token_type='Bearer'):
        self.token, self.token_type = token, token_type or 'Bearer'
    def request(self, method, path, *, params=None, json=None, authenticated=True):
        method = method.upper(); safe = method in {'GET','HEAD','OPTIONS','DELETE'}
        last_error = None
        for attempt in range((self.max_retries + 1) if safe else 1):
            for base in list(self.base_urls):
                headers = {'Accept':'application/json','X-Request-Id':str(uuid.uuid4())}
                if self.token:
                    headers['Authorization'] = self.token if self.token.startswith('dfsha_live_') else f'{self.token_type} {self.token}'
                try:
                    r = self.session.request(method, urljoin(base, path.lstrip('/')), params=params, json=json,
                                             headers=headers, timeout=self.timeout, verify=self.verify_tls)
                except requests.RequestException as e:
                    last_error = e; continue
                try: body = r.json() if r.content else {}
                except ValueError: body = {'detail':r.text[:1000]}
                if r.status_code == 503 and safe:
                    leader = body.get('leader_url')
                    if leader and leader.rstrip('/')+'/' not in self.base_urls: self.base_urls.insert(0, leader.rstrip('/')+'/')
                    last_error = APIError(503, body.get('detail','NameNode unavailable'), body.get('code'), body); continue
                if r.ok: return body
                cls = AuthenticationError if r.status_code in (401,403) else APIError
                raise cls(r.status_code, str(body.get('detail') or body.get('message') or r.reason), body.get('code') or body.get('error_code'), body)
            if attempt < (self.max_retries if safe else 0): time.sleep(min(.2 * 2**attempt, 1))
        raise APIError(503, f'Could not reach any NameNode: {last_error or "no response"}')
