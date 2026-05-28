"""
Centaur Prism — NSE stocks refracted into spectrums.
Start with: python run.py   →   then open http://localhost:5001
Educational tool only. Not investment advice. DYOR.
"""
import sys
import os
import threading
import webbrowser

# Ensure app.py is importable when launched from a different cwd (portable Python)
_here = os.path.dirname(os.path.abspath(__file__))
if _here not in sys.path:
    sys.path.insert(0, _here)


def _build_merged_ca_bundle():
    """
    Combine certifi's PEM with the Windows certificate store, write to a temp
    file, and point SSL libs at it. Required when running as a frozen exe
    inside a corporate network that does TLS interception.
    """
    try:
        import ssl, tempfile, certifi
        from base64 import encodebytes

        pem_parts = []
        with open(certifi.where(), 'rb') as f:
            pem_parts.append(f.read())

        # Walk Windows trust stores
        if hasattr(ssl, 'enum_certificates'):
            for store in ('ROOT', 'CA'):
                try:
                    for cert_bytes, encoding, trust in ssl.enum_certificates(store):
                        # Skip distrusted certs (trust is a frozenset of EKU OIDs;
                        # True means trusted for all purposes)
                        if trust is False:
                            continue
                        if encoding == 'x509_asn':
                            b64 = encodebytes(cert_bytes).decode('ascii')
                            pem = '-----BEGIN CERTIFICATE-----\n' + b64 + '-----END CERTIFICATE-----\n'
                            pem_parts.append(pem.encode('ascii'))
                except Exception:
                    pass

        out_dir = tempfile.gettempdir()
        out_path = os.path.join(out_dir, 'nse_analyzer_ca_bundle.pem')
        with open(out_path, 'wb') as f:
            f.write(b'\n'.join(pem_parts))

        os.environ['SSL_CERT_FILE']      = out_path
        os.environ['REQUESTS_CA_BUNDLE'] = out_path
        os.environ['CURL_CA_BUNDLE']     = out_path
        return out_path
    except Exception as e:
        print(f'  [WARN] could not build merged CA bundle: {e}')
        return None


# In a PyInstaller-frozen exe, merge Windows cert store with certifi.
# Must run BEFORE importing yfinance (which imports curl_cffi which caches
# the CA path at import time).
if getattr(sys, 'frozen', False):
    _build_merged_ca_bundle()

from app import app


def _open_browser():
    # Wait briefly so the server is listening before the browser opens
    import time
    time.sleep(1.5)
    try:
        webbrowser.open('http://localhost:5001')
    except Exception:
        pass


if __name__ == '__main__':
    print("\n  ============================================")
    print("   CENTAUR PRISM")
    print("   NSE stocks refracted into spectrums")
    print("  ============================================")
    print("   Open -> http://localhost:5001")
    print("   Educational only. Not investment advice.")
    print("   Press Ctrl+C to stop the server.\n")

    # In a PyInstaller bundle (frozen), auto-open the browser
    if getattr(sys, 'frozen', False):
        threading.Thread(target=_open_browser, daemon=True).start()

    # use_reloader=False is required when frozen
    app.run(debug=False, port=5001, host='0.0.0.0', use_reloader=False)
