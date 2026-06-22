import getpass
import io
import posixpath
from datetime import datetime
from ftplib import FTP
from pathlib import Path

from .config import require_value
from .dependencies import import_paramiko


def prompt_password(label):
    return getpass.getpass(f"{label}: ")


def remote_dir(item):
    return item.get("remote-dir") or item.get("remote_dir") or "."


def ftp_timeout(item):
    return float(item.get("timeout") or item.get("ftp_timeout") or 30)


def list_ftp(item):
    host = require_value(item, "host")
    username = require_value(item, "username")
    port = int(item.get("port") or 21)
    directory = remote_dir(item)
    timeout = ftp_timeout(item)
    password = item.get("password")
    if password is None:
        password = prompt_password(f"FTP password for {username}@{host}")

    with FTP(timeout=timeout) as ftp:
        ftp.connect(host, port, timeout=timeout)
        ftp.sock.settimeout(timeout)
        ftp.login(username, password)
        ftp.sock.settimeout(timeout)
        ftp.cwd(directory)
        ftp.sock.settimeout(timeout)
        return [posixpath.join(ftp.pwd(), name) for name in ftp.nlst()]


def connect_ftp(item):
    host = require_value(item, "host")
    username = require_value(item, "username")
    port = int(item.get("port") or 21)
    timeout = ftp_timeout(item)
    password = item.get("password")
    if password is None:
        password = prompt_password(f"FTP password for {username}@{host}")

    print(f"FTP connect {host}:{port}", flush=True)
    ftp = FTP(timeout=timeout)
    ftp.connect(host, port, timeout=timeout)
    ftp.sock.settimeout(timeout)
    print(f"FTP login {username}", flush=True)
    ftp.login(username, password)
    ftp.sock.settimeout(timeout)
    print(f"FTP cwd {remote_dir(item)}", flush=True)
    ftp.cwd(remote_dir(item))
    ftp.sock.settimeout(timeout)
    return ftp


def ftp_name(entry):
    return posixpath.basename(entry.rstrip("/"))


def remote_file_path(item, file_path):
    file_path = str(file_path).strip()
    if not file_path:
        raise ValueError("remote file path is empty")
    if posixpath.isabs(file_path):
        return file_path

    directory = remote_dir(item)
    if directory in ("", "."):
        return file_path
    return posixpath.join(directory, file_path)


def decode_remote_text(raw):
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def read_ftp_file(item, file_path):
    with connect_ftp(item) as ftp:
        buffer = io.BytesIO()
        ftp.retrbinary(f"RETR {file_path}", buffer.write)
        return buffer.getvalue()


def read_sftp_file(item, file_path):
    paramiko = import_paramiko()
    host = require_value(item, "host")
    username = require_value(item, "username")
    port = int(item.get("port") or 22)
    password = item.get("password")
    private_key = None

    key_path = item.get("key_path") or item.get("key")
    if key_path:
        private_key = load_private_key(
            paramiko, key_path, item.get("key_passphrase")
        )
    elif password is None:
        password = prompt_password(f"SFTP password for {username}@{host}")

    path = remote_file_path(item, file_path)
    transport = paramiko.Transport((host, port))
    try:
        transport.connect(
            username=username, password=password, pkey=private_key
        )
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            with sftp.open(path, "rb") as handle:
                return handle.read()
    finally:
        transport.close()


def read_remote_text_file(item, file_path):
    protocol = (item.get("protocol") or "ftp").lower()
    if protocol == "ftp":
        raw = read_ftp_file(item, file_path)
    elif protocol == "sftp":
        raw = read_sftp_file(item, file_path)
    else:
        name = item.get("type", "unnamed")
        raise ValueError(f"{name}: unsupported protocol '{protocol}'")
    return decode_remote_text(raw)


def ensure_ftp_dir(ftp, directory):
    try:
        ftp.mkd(directory)
    except Exception:
        pass


def ftp_path_exists(ftp, path):
    try:
        ftp.size(path)
        return True
    except Exception:
        return False


def archive_ftp_file(ftp, source, directory):
    ensure_ftp_dir(ftp, directory)
    target = posixpath.join(directory, ftp_name(source))
    if ftp_path_exists(ftp, target):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        target = posixpath.join(directory, f"{timestamp}-{ftp_name(source)}")

    print(f"FTP rename {source} -> {target}", flush=True)
    ftp.rename(source, target)
    return target


def download_ftp_file(item, filename):
    with connect_ftp(item) as ftp:
        zip_buffer = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", zip_buffer.write)
        zip_buffer.seek(0)
        return zip_buffer


def archive_ftp_item(item, filename, directory):
    with connect_ftp(item) as ftp:
        return archive_ftp_file(ftp, filename, directory)


def load_private_key(paramiko, key_path, passphrase):
    key_path = str(Path(key_path).expanduser())
    key_classes = tuple(
        key_class
        for key_class in (
            getattr(paramiko, "Ed25519Key", None),
            getattr(paramiko, "RSAKey", None),
            getattr(paramiko, "ECDSAKey", None),
            getattr(paramiko, "DSSKey", None),
        )
        if key_class is not None
    )

    last_error = None
    for key_class in key_classes:
        try:
            return key_class.from_private_key_file(
                key_path, password=passphrase
            )
        except paramiko.PasswordRequiredException:
            passphrase = prompt_password("SSH key passphrase")
            try:
                return key_class.from_private_key_file(
                    key_path, password=passphrase
                )
            except paramiko.SSHException as exc:
                last_error = exc
        except paramiko.SSHException as exc:
            last_error = exc

    message = f"Could not read SSH private key: {key_path}"
    if last_error:
        message = f"{message} ({last_error})"
    raise ValueError(message)


def list_sftp(item):
    paramiko = import_paramiko()
    host = require_value(item, "host")
    username = require_value(item, "username")
    port = int(item.get("port") or 22)
    directory = remote_dir(item)
    password = item.get("password")
    private_key = None

    key_path = item.get("key_path") or item.get("key")
    if key_path:
        private_key = load_private_key(
            paramiko, key_path, item.get("key_passphrase")
        )
    elif password is None:
        password = prompt_password(f"SFTP password for {username}@{host}")

    transport = paramiko.Transport((host, port))
    try:
        transport.connect(
            username=username, password=password, pkey=private_key
        )
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            return [
                posixpath.join(directory, item.filename)
                for item in sftp.listdir_attr(directory)
            ]
    finally:
        transport.close()


def list_remote(item):
    protocol = (item.get("protocol") or "ftp").lower()
    if protocol == "ftp":
        return list_ftp(item)
    if protocol == "sftp":
        return list_sftp(item)
    name = item.get("type", "unnamed")
    raise ValueError(f"{name}: unsupported protocol '{protocol}'")
