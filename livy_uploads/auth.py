#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
import logging
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Union
import subprocess

from requests.auth import AuthBase
from livy_uploads.utils import assert_type
try:
    from requests_gssapi import HTTPSPNEGOAuth
except ImportError:
    HTTPSPNEGOAuth = None


LOGGER = logging.getLogger(__name__)



class Authenticator:
    def __call__(self) -> Optional[AuthBase]:
        return None

    @classmethod
    def from_config(cls, config: Optional[Mapping[str, Any]]) -> 'Authenticator':
        if not config or config.get('type') != 'kerberos':
            return Authenticator()

        return KerberosAuthenticator(
            principal=assert_type(config.get('principal'), Optional[str]),
            keytab=assert_type(config.get('keytab'), Optional[str]),
            password=assert_type(config.get('password'), Optional[str]),
            krb5_config=assert_type(config.get('krb5_config'), Optional[str]),
            krb5_cache=assert_type(config.get('krb5_cache'), Optional[str]),
            mutual_authentication=getattr(MutualAuth, (assert_type(config.get('mutual_authentication'), Optional[str]) or 'OPTIONAL').upper()),
            target_name=assert_type(config.get('target_name'), Optional[str]),
            delegate=assert_type(config.get('delegate'), Optional[bool]),
            opportunistic_auth=assert_type(config.get('opportunistic_auth'), Optional[bool]),
        )


class MutualAuth(Enum):
    REQUIRED = 1
    OPTIONAL = 2
    DISABLED = 3


class KerberosAuthenticator(Authenticator):
    def __init__(
        self,
        principal: Optional[str] = None,
        keytab: Optional[Union[str, Path]] = None,
        password: Optional[str] = None,
        krb5_config: Optional[Union[str, Path]] = None,
        krb5_cache: Optional[Union[str, Path]] = None,
        mutual_authentication: Optional[MutualAuth] = None,
        target_name: Optional[str] = None,
        delegate: bool = False,
        opportunistic_auth: bool = False,
    ):
        if HTTPSPNEGOAuth is None:
            raise ImportError('requests-gssapi is not installed')

        self.principal = principal or None

        if self.principal:
            if not keytab and not password:
                raise ValueError('Either keytab or password must be provided')
            if keytab and password:
                raise ValueError('Only one of keytab or password must be provided')

        self.keytab = keytab or None
        self.password = password or None
        self.krb5_config = Path(krb5_config or '/etc/krb5.conf')
        self.krb5_cache = Path(krb5_cache or 'var/krb5_cache')
        self.mutual_authentication = mutual_authentication or None
        self.target_name = target_name or None
        self.delegate = delegate or None
        self.opportunistic_auth = opportunistic_auth or None

    def __call__(self) -> AuthBase:
        assert HTTPSPNEGOAuth is not None, 'requests-gssapi is not installed'

        LOGGER.info('initializing environment')
        self.krb5_cache.absolute().parent.mkdir(parents=True, exist_ok=True)
        os.environ['KRB5_CONFIG'] = str(self.krb5_config.absolute())
        os.environ['KRB5CCNAME'] = str(self.krb5_cache.absolute())

        if not self.principal:
            LOGGER.info('using cached credentials')
        elif self.keytab:
            LOGGER.info('logging with principal=%s and keytab=%s', self.principal, self.keytab)
            subprocess.run(['kinit', '-Vkt', self.keytab, self.principal], check=True)
        else:
            LOGGER.info('logging with principal=%s and password', self.principal)
            stdin = (self.password + '\n').encode('utf8')
            subprocess.run(['kinit', '-V', self.principal], check=True, input=stdin)

        kwargs = dict(
            mutual_authentication=self.mutual_authentication.value,
            target_name=self.target_name,
            delegate=self.delegate,
            opportunistic_auth=self.opportunistic_auth,
        )
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        return HTTPSPNEGOAuth(**kwargs)
