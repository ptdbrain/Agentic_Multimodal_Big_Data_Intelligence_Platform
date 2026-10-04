"""Standalone S3/MinIO compatible server for SentinelAI.
Implements standard S3 REST API (Bucket, PutObject, GetObject, ListObjectsV2, HeadObject, DeleteObject)
allowing MinIOBackend and Spark S3A to perform 100% real network S3 storage operations on localhost:9000.
"""
import os
import io
import time
import shutil
import hashlib
import datetime
import urllib.parse
import xml.etree.ElementTree as ET
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Dict, Any, Optional

STORAGE_ROOT = Path(__file__).resolve().parent.parent / "storage" / "minio_root"
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)


class S3RequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def _send_xml(self, status: int, root: ET.Element):
        xml_bytes = ET.tostring(root, encoding="utf-8")
        data = b'<?xml version="1.0" encoding="UTF-8"?>\n' + xml_bytes
        self.send_response(status)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.wfile.flush()
        self.close_connection = True

    def _send_error(self, status: int, code: str, message: str, resource: str):
        root = ET.Element("Error")
        ET.SubElement(root, "Code").text = code
        ET.SubElement(root, "Message").text = message
        ET.SubElement(root, "Resource").text = resource
        self._send_xml(status, root)

    @staticmethod
    def _get_etag(file_path: Path) -> str:
        try:
            if file_path.is_file():
                return f'"{hashlib.md5(file_path.read_bytes()).hexdigest()}"'
        except Exception:
            pass
    @staticmethod
    def _decode_aws_chunked(data: bytes) -> bytes:
        out = bytearray()
        idx = 0
        total = len(data)
        while idx < total:
            nl = data.find(b"\r\n", idx)
            if nl == -1:
                break
            header = data[idx:nl]
            if b";chunk-signature=" not in header:
                return data
            chunk_size_str = header.split(b";")[0].strip()
            try:
                chunk_size = int(chunk_size_str, 16)
            except ValueError:
                return data
            if chunk_size == 0:
                break
            chunk_start = nl + 2
            chunk_end = chunk_start + chunk_size
            out.extend(data[chunk_start:chunk_end])
            idx = chunk_end + 2
        return bytes(out)

    def do_HEAD(self):
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path).lstrip("/")
        parts = path.split("/", 1)
        bucket = parts[0] if parts else ""
        obj_key = parts[1] if len(parts) > 1 else ""

        bucket_dir = STORAGE_ROOT / bucket
        if not bucket_dir.exists():
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return

        if not obj_key:
            # Bucket exists
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return

        obj_file = bucket_dir / obj_key
        if obj_key.endswith("/"):
            if obj_file.is_dir() or (bucket_dir / obj_key.rstrip("/")).is_dir():
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.send_header("Content-Type", "application/x-directory")
                self.send_header("ETag", f'"{hashlib.md5(obj_key.encode()).hexdigest()}"')
                self.send_header("Connection", "close")
                self.end_headers()
                return
            else:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.send_header("Connection", "close")
                self.end_headers()
                return

        if not obj_file.exists() or not obj_file.is_file():
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return

        stat = obj_file.stat()
        self.send_response(200)
        self.send_header("Content-Length", str(stat.st_size))
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("ETag", self._get_etag(obj_file))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Connection", "close")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        path = urllib.parse.unquote(parsed.path).lstrip("/")
        parts = path.split("/", 1)
        bucket = parts[0] if parts else ""
        obj_key = parts[1] if len(parts) > 1 else ""

        bucket_dir = STORAGE_ROOT / bucket
        if not bucket_dir.exists():
            self._send_error(404, "NoSuchBucket", f"The specified bucket does not exist: {bucket}", f"/{bucket}")
            return

        # 1. Bucket Location query (?location=)
        if "location" in params or (not obj_key and not any(k in params for k in ("prefix", "list-type", "delimiter"))):
            root = ET.Element("LocationConstraint", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
            root.text = "us-east-1"
            self._send_xml(200, root)
            return

        # 2. List Objects (V1 or V2)
        if not obj_key or any(k in params for k in ("prefix", "list-type", "delimiter")):
            prefix = urllib.parse.unquote(params.get("prefix", [""])[0])
            delimiter = params.get("delimiter", [None])[0]

            root = ET.Element("ListBucketResult", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
            ET.SubElement(root, "Name").text = bucket
            ET.SubElement(root, "Prefix").text = prefix
            ET.SubElement(root, "MaxKeys").text = "10000"
            ET.SubElement(root, "IsTruncated").text = "false"

            matching_files = []
            common_prefixes = set()

            if bucket_dir.exists():
                if delimiter and prefix and prefix.endswith("/"):
                    pref_dir = bucket_dir / prefix.rstrip("/")
                    if pref_dir.exists() and pref_dir.is_dir():
                        matching_files.append((prefix, pref_dir))

                for f in bucket_dir.rglob("*"):
                    rel = f.relative_to(bucket_dir).as_posix()
                    if f.is_dir():
                        if delimiter:
                            dir_key = rel + "/"
                            if not prefix or dir_key.startswith(prefix):
                                remainder = dir_key[len(prefix):]
                                if remainder and delimiter in remainder:
                                    cp = prefix + remainder.split(delimiter)[0] + delimiter
                                    common_prefixes.add(cp)
                    elif f.is_file():
                        if not prefix or rel.startswith(prefix):
                            if delimiter:
                                remainder = rel[len(prefix):]
                                if delimiter in remainder:
                                    cp = prefix + remainder.split(delimiter)[0] + delimiter
                                    common_prefixes.add(cp)
                                else:
                                    matching_files.append((rel, f))
                            else:
                                matching_files.append((rel, f))

            ET.SubElement(root, "KeyCount").text = str(len(matching_files) + len(common_prefixes))
            for rel, f in sorted(matching_files, key=lambda x: x[0]):
                stat = f.stat()
                is_dir_entry = f.is_dir()
                c = ET.SubElement(root, "Contents")
                ET.SubElement(c, "Key").text = rel
                mtime_iso = datetime.datetime.fromtimestamp(stat.st_mtime, tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                ET.SubElement(c, "LastModified").text = mtime_iso
                ET.SubElement(c, "ETag").text = '"d41d8cd98f00b204e9800998ecf8427e"' if is_dir_entry else self._get_etag(f)
                ET.SubElement(c, "Size").text = "0" if is_dir_entry else str(stat.st_size)
                ET.SubElement(c, "StorageClass").text = "STANDARD"

            for cp in sorted(list(common_prefixes)):
                c = ET.SubElement(root, "CommonPrefixes")
                ET.SubElement(c, "Prefix").text = cp

            self._send_xml(200, root)
            return

        # 3. Get Object
        obj_file = bucket_dir / obj_key
        if not obj_file.exists() or not obj_file.is_file():
            self._send_error(404, "NoSuchKey", f"The specified key does not exist: {obj_key}", f"/{bucket}/{obj_key}")
            return

        data = obj_file.read_bytes()
        total_len = len(data)
        range_header = self.headers.get("Range")

        if range_header and range_header.startswith("bytes="):
            r = range_header[6:].strip()
            if "," not in r:
                parts = r.split("-")
                if parts[0] == "":
                    suffix = int(parts[1])
                    start = max(0, total_len - suffix)
                    end = total_len - 1
                elif parts[1] == "":
                    start = int(parts[0])
                    end = total_len - 1
                else:
                    start = int(parts[0])
                    end = min(total_len - 1, int(parts[1]))

                chunk = data[start:end + 1]
                self.send_response(206)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Range", f"bytes {start}-{end}/{total_len}")
                self.send_header("Content-Length", str(len(chunk)))
                self.send_header("ETag", self._get_etag(obj_file))
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(chunk)
                return

        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(total_len))
        self.send_header("ETag", self._get_etag(obj_file))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)

    def do_PUT(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        path = urllib.parse.unquote(parsed.path).lstrip("/")
        parts = path.split("/", 1)
        bucket = parts[0] if parts else ""
        obj_key = parts[1] if len(parts) > 1 else ""

        bucket_dir = STORAGE_ROOT / bucket
        if not bucket_dir.exists():
            bucket_dir.mkdir(parents=True, exist_ok=True)

        if not obj_key:
            # Create Bucket (make_bucket)
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return

        copy_source = self.headers.get("x-amz-copy-source")
        if copy_source:
            # S3 CopyObject (used by S3A rename)
            copy_source = urllib.parse.unquote(copy_source).lstrip("/")
            src_parts = copy_source.split("/", 1)
            src_bucket = src_parts[0] if src_parts else ""
            src_key = src_parts[1] if len(src_parts) > 1 else ""

            src_file = STORAGE_ROOT / src_bucket / src_key
            dest_file = bucket_dir / obj_key
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            if src_file.exists():
                if src_file.is_file():
                    shutil.copy2(src_file, dest_file)
                elif src_file.is_dir():
                    shutil.copytree(src_file, dest_file, dirs_exist_ok=True)

            etag = self._get_etag(dest_file) if dest_file.exists() and dest_file.is_file() else f'"{hashlib.md5(b"").hexdigest()}"'
            root = ET.Element("CopyObjectResult", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
            ET.SubElement(root, "LastModified").text = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            ET.SubElement(root, "ETag").text = etag
            self._send_xml(200, root)
            return

        if obj_key.endswith("/"):
            dir_path = bucket_dir / obj_key.rstrip("/")
            dir_path.mkdir(parents=True, exist_ok=True)
            etag = "d41d8cd98f00b204e9800998ecf8427e"
            self.send_response(200)
            self.send_header("ETag", f'"{etag}"')
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return

        # Put Object or Part
        content_len = int(self.headers.get("Content-Length", 0))
        data = self.rfile.read(content_len) if content_len > 0 else b""
        if b";chunk-signature=" in data[:200]:
            data = self._decode_aws_chunked(data)

        target_file = bucket_dir / obj_key
        target_file.parent.mkdir(parents=True, exist_ok=True)
        if "partNumber" in params and int(params.get("partNumber", [1])[0]) > 1:
            with open(target_file, "ab") as f:
                f.write(data)
        else:
            target_file.write_bytes(data)

        etag = hashlib.md5(data).hexdigest()
        self.send_response(200)
        self.send_header("ETag", f'"{etag}"')
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        path = urllib.parse.unquote(parsed.path).lstrip("/")
        parts = path.split("/", 1)
        bucket = parts[0] if parts else ""
        obj_key = parts[1] if len(parts) > 1 else ""

        bucket_dir = STORAGE_ROOT / bucket
        if not bucket_dir.exists():
            bucket_dir.mkdir(parents=True, exist_ok=True)

        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len) if content_len > 0 else b""
        if b";chunk-signature=" in body[:200]:
            body = self._decode_aws_chunked(body)

        # 1. Multi-object delete (POST /bucket?delete)
        if "delete" in params:
            root = ET.Element("DeleteResult", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
            try:
                del_tree = ET.fromstring(body)
                for elem in del_tree.iter():
                    if elem.tag.endswith("Key"):
                        k = (elem.text or "").strip()
                        if k:
                            target = bucket_dir / k.rstrip("/")
                            if not k.endswith("/") and target.is_file():
                                target.unlink(missing_ok=True)
                            elif target.is_dir():
                                try:
                                    target.rmdir()
                                except OSError:
                                    pass
                            d = ET.SubElement(root, "Deleted")
                            ET.SubElement(d, "Key").text = k
            except Exception:
                pass
            self._send_xml(200, root)
            return

        # 2. Initiate multipart upload (?uploads)
        if "uploads" in params:
            upload_id = hashlib.md5(f"{obj_key}_{time.time()}".encode()).hexdigest()
            root = ET.Element("InitiateMultipartUploadResult", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
            ET.SubElement(root, "Bucket").text = bucket
            ET.SubElement(root, "Key").text = obj_key
            ET.SubElement(root, "UploadId").text = upload_id
            self._send_xml(200, root)
            return

        # 3. Complete multipart upload (?uploadId=...)
        if "uploadId" in params:
            root = ET.Element("CompleteMultipartUploadResult", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
            ET.SubElement(root, "Location").text = f"http://127.0.0.1:9000/{bucket}/{obj_key}"
            ET.SubElement(root, "Bucket").text = bucket
            ET.SubElement(root, "Key").text = obj_key
            ET.SubElement(root, "ETag").text = f'"{hashlib.md5(obj_key.encode()).hexdigest()}"'
            self._send_xml(200, root)
            return

        self.send_response(200)
        self.send_header("Connection", "close")
        self.end_headers()

    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path).lstrip("/")
        parts = path.split("/", 1)
        bucket = parts[0] if parts else ""
        obj_key = parts[1] if len(parts) > 1 else ""

        bucket_dir = STORAGE_ROOT / bucket
        if bucket_dir.exists() and obj_key:
            target = bucket_dir / obj_key.rstrip("/")
            if not obj_key.endswith("/") and target.is_file():
                target.unlink(missing_ok=True)
            elif target.is_dir():
                try:
                    target.rmdir()
                except OSError:
                    pass

        self.send_response(204)
        self.send_header("Connection", "close")
        self.end_headers()

    def log_message(self, format, *args):
        print(f"[S3Server] {self.command} {self.path} - {format % args}", flush=True)


def run_s3_server(port: int = 9000):
    server = ThreadingHTTPServer(("127.0.0.1", port), S3RequestHandler)
    print(f"Standalone S3 Server listening on http://127.0.0.1:{port} (Root: {STORAGE_ROOT})")
    server.serve_forever()


if __name__ == "__main__":
    run_s3_server()
