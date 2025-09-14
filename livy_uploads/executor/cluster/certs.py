#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Code to manage the certificates for the server and workers.
'''

__all__ = ('CertManager',)


from contextlib import ExitStack
import logging
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
from typing import Optional, List, Tuple


HOSTNAME_PATTERN = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$')

LOGGER = logging.getLogger(__name__)


class CertManager:
    """
    Manages the certificates for the server and workers.
    """

    def __init__(self, basedir: Optional[str] = None):
        self.basedir = basedir or 'var'
        self.stack = ExitStack()
        self.var_path: Optional[Path] = None

    def setup(self) -> None:
        tmpdir = self.stack.enter_context(tempfile.TemporaryDirectory(dir=self.basedir))
        self.var_path = Path(tmpdir)

        self._make_ca()

    def close(self) -> None:
        if self.stack:
            self.stack.close()
            self.stack = None

    @property
    def ca_path(self) -> Path:
        assert self.var_path
        return self.var_path / 'ca.pem'

    @property
    def key_path(self) -> Path:
        assert self.var_path
        return self.var_path / 'ca.key'

    @property
    def ca_conf_path(self) -> Path:
        assert self.var_path
        return self.var_path / 'ca.cnf'

    @property
    def cert_conf_path(self) -> Path:
        assert self.var_path
        return self.var_path / 'cert.cnf'

    def get_cert_paths(self, name: str) -> Tuple[Path, Path]:
        assert self.var_path

        parts = name.split('@', maxsplit=1)
        for p in parts:
            if not HOSTNAME_PATTERN.match(p):
                raise ValueError(f'Invalid name: {name!r}')

        name = name.replace('@', '-')
        return self.var_path / f'cert-{name}.pem', self.var_path / f'cert-{name}.key'

    def make_key(self, name: str, key_size: int = 2048, force: bool = False) -> Path:
        """
        Creates a new client RSA key.
        """
        _, key_path = self.get_cert_paths(name)
        if key_path.exists() and not force:
            LOGGER.info('key already exists at %s', key_path)
            return key_path

        LOGGER.info('creating new key at %s', key_path)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([
            'openssl', 'genrsa', '-out', str(key_path), str(key_size),
        ], check=True, stdout=subprocess.DEVNULL)

        return key_path

    def make_request(self, name: str, hostnames: Optional[List[str]] = None, ips: Optional[List[str]] = None, force: bool = False) -> Tuple[Path, Path]:
        """
        Creates a new certificate request.

        Args:
            name: The name of the certificate.
            hostnames: The hostnames of the certificate.
            ips: The IPs of the certificate.
            force: If True, the certificate request will be created even if it already exists.

        Returns:
            A tuple of the certificate request path and the key path.
        """
        parts = name.split('@', maxsplit=1)
        for p in parts:
            if not HOSTNAME_PATTERN.match(p):
                raise ValueError(f'Invalid name: {name!r}')

        hostnames = hostnames or []
        ips = ips or []
        if '@' not in name and name not in hostnames:
            hostnames = [name] + hostnames

        assert self.ca_path.exists()
        assert self.key_path.exists()

        key_path = self.make_key(name, force=force)

        conf_path = self.var_path / f'cert-{name}.cnf'
        LOGGER.info('creating certificate configuration at %s', conf_path)

        # Build alt names section
        alt_names = []
        for i, hostname in enumerate(hostnames):
            alt_names.append(f'DNS.{2*i+1} = {hostname}')
            alt_names.append(f'DNS.{2*i+2} = *.{hostname}')
        for i, ip in enumerate(ips):
            alt_names.append(f'IP.{i+1} = {ip}')

        # Only include subjectAltName if we have alt names
        v3_req_section = textwrap.dedent('''
            [v3_req]
            basicConstraints = CA:FALSE
            keyUsage = nonRepudiation, digitalSignature, keyEncipherment
        ''')
        
        if alt_names:
            v3_req_section += 'subjectAltName = @alt_names\n'
        
        conf = textwrap.dedent(f'''
            [req]
            default_bits = 2048
            default_md = sha256
            prompt = no
            distinguished_name = req_dn
            x509_extensions = v3_req

            [req_dn]
            C = US
            ST = California
            L = San Francisco
            O = Livy
            OU = Livy
            CN = {name}

            {v3_req_section}
        ''')
        
        if alt_names:
            conf += '\n[alt_names]\n'
            conf += '\n'.join(alt_names) + '\n'

        conf_path.write_text(conf)

        csr_path = self.var_path / f'cert-{name.replace("@", "-")}.csr'
        LOGGER.info('creating new CSR at %s', csr_path)
        subprocess.run([
            'openssl', 'req', '-new', '-key', str(key_path), '-out', str(csr_path), '-config', str(conf_path),
        ], check=True, stdout=subprocess.DEVNULL)

        return csr_path, key_path

    def sign_request(self, csr_path: Path, force: bool = False) -> Path:
        """
        Signs a certificate request.

        Args:
            csr_path: The path to the certificate request.

        Returns:
            The path to the signed certificate.
        """
        cert_path = self.var_path / f'cert-{csr_path.stem}.pem'
        if cert_path.exists() and not force:
            LOGGER.info('certificate already exists at %s', cert_path)
            return cert_path

        LOGGER.info('signing request at %s and generating certificate at %s', csr_path, cert_path)
        subprocess.run([
            'openssl', 'x509', '-req', '-in', str(csr_path), '-out', str(cert_path), '-CA', str(self.ca_path), '-CAkey', str(self.key_path), '-CAcreateserial', '-days', '365',
        ], check=True, stdout=subprocess.DEVNULL)

        return cert_path

    def make_cert(self, name: str, hostnames: Optional[List[str]] = None, ips: Optional[List[str]] = None, force: bool = False) -> Tuple[Path, Path]:
        """
        Creates a new key, CSR and signed certificate.

        Args:
            name: The name of the certificate.
            hostnames: The hostnames of the certificate.
            ips: The IPs of the certificate.
            force: If True, the certificate request will be created even if it already exists.

        Returns:
            A tuple of the certificate path and the key path.
        """
        csr_path, key_path = self.make_request(name, hostnames, ips, force)
        cert_path = self.sign_request(csr_path, force)
        return cert_path, key_path

    def _make_ca(self):
        self.ca_conf_path.write_text(textwrap.dedent('''
            [req]
            default_bits = 2048
            default_md = sha256
            prompt = no
            distinguished_name = req_dn
            x509_extensions = v3_req

            [req_dn]
            C = US
            ST = California
            L = San Francisco
            O = Livy
            OU = Livy
            CN = livy.ai

            [v3_req]
            basicConstraints = CA:TRUE
            keyUsage = keyCertSign, cRLSign
        '''))

        subprocess.run([
            'openssl', 'req', '-x509', '-new', '-nodes', '-keyout', str(self.key_path), '-out', str(self.ca_path), '-days', '365', '-config', str(self.ca_conf_path),
        ], check=True, stdout=subprocess.DEVNULL)
