import requests
import threading
import time
import re
import sys
import os
import signal
import random
import socket
import ipaddress
import struct
import base64
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import defaultdict
from datetime import datetime
from queue import Queue

try:
    import socks
    SOCKS_AVAILABLE = True
except ImportError:
    SOCKS_AVAILABLE = False

GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
MAGENTA = '\033[95m'
WHITE = '\033[97m'
BRIGHT = '\033[1m'
RESET = '\033[0m'
CLEAR_LINE = '\033[2K\r'

CHECK_TIMEOUT = 12
CONNECT_TIMEOUT = 6
MAX_RETRIES = 3
MAX_THREADS = 200
BATCH_SIZE = 100
SAVE_INTERVAL = 500
DNS_TIMEOUT = 3
PROXY_VALIDATE_IP = True

PRIMARY_URLS = [
    "http://httpbin.org/ip", "https://httpbin.org/ip",
    "http://api.ipify.org", "https://api.ipify.org",
    "http://icanhazip.com", "https://icanhazip.com",
    "http://checkip.amazonaws.com", "https://checkip.amazonaws.com",
    "http://ipv4.whatismyip.akamai.com",
    "http://ifconfig.me/ip", "https://ifconfig.me/ip",
    "http://ident.me", "https://ident.me",
    "http://myip.dnsomatic.com",
    "http://ip.42.pl/raw", "https://ip.42.pl/raw",
    "http://ip4.me", "https://ip4.me",
    "http://ip6.me", "https://ip6.me",
    "http://ipv4.icanhazip.com", "https://ipv4.icanhazip.com",
    "http://ipv6.icanhazip.com", "https://ipv6.icanhazip.com",
    "http://wtfismyip.com/text", "https://wtfismyip.com/text",
    "http://ip.dnsexit.com", "https://ip.dnsexit.com",
    "http://ipv4.dyndns.org", "http://ipv6.dyndns.org",
    "http://checkipv4.dyndns.org", "http://checkipv6.dyndns.org",
    "http://ipv4.myip.dk", "http://ipv6.myip.dk",
    "http://ipv4.nsupdate.info/myip", "http://ipv6.nsupdate.info/myip"
]

SECONDARY_URLS = [
    "http://ip-api.com/line/", "https://ip-api.com/line/",
    "http://ip-api.com/json", "https://ip-api.com/json",
    "http://ipinfo.io/ip", "https://ipinfo.io/ip",
    "http://ipecho.net/plain", "https://ipecho.net/plain",
    "http://www.trackip.net/ip", "https://www.trackip.net/ip",
    "http://jsonip.com", "https://jsonip.com",
    "http://ipwho.is/", "https://ipwho.is/",
    "http://geoip-db.com/json/", "https://geoip-db.com/json/",
    "http://extreme-ip-lookup.com/json/", "https://extreme-ip-lookup.com/json/",
    "http://ip.jsontest.com", "https://ip.jsontest.com",
    "http://l2.io/ip.js", "https://l2.io/ip.js",
    "http://ip.myresearch.net", "http://ipv4.myresearch.net",
    "http://ipv6.myresearch.net", "http://myip.bit.nl",
    "http://ip.mooo.com", "http://ip.bit.nl",
    "http://whatismyip.gg", "http://ip.anysrc.net",
    "http://ip.bbking.me", "http://ip.chocolatkey.com",
    "http://ip.dnsexit.com", "http://ip.sb",
    "https://ip.sb", "http://ipv4.ip.sb", "https://ipv4.ip.sb",
    "http://ipv6.ip.sb", "https://ipv6.ip.sb",
    "http://ip.gs", "http://ip.oxylabs.io",
    "http://ip.seeip.org", "https://ip.seeip.org",
    "http://ip.t0.vc", "http://ipinfo.io", "https://ipinfo.io",
    "http://ip-api.com", "https://ip-api.com"
]

FALLBACK_URLS = [
    "http://httpbin.org/get", "https://httpbin.org/get",
    "http://httpbin.org/user-agent", "https://httpbin.org/user-agent",
    "http://httpbin.org/headers", "https://httpbin.org/headers",
    "http://httpbin.org/anything", "https://httpbin.org/anything",
    "http://jsonip.mit.edu", "https://jsonip.mit.edu",
    "http://ipapi.co/ip", "https://ipapi.co/ip",
    "http://ipapi.co/json", "https://ipapi.co/json",
    "http://ipinfo.io/json", "https://ipinfo.io/json",
    "http://ip-api.com/json", "https://ip-api.com/json",
    "http://freegeoip.app/json/", "https://freegeoip.app/json/",
    "http://geoip.nekudo.com/api/", "https://geoip.nekudo.com/api/",
    "http://ipstack.com/check", "https://ipstack.com/check",
    "http://ipapi.com/ip", "https://ipapi.com/ip",
    "http://ipdata.co/ip", "https://ipdata.co/ip",
    "http://ipify.org/ip", "https://ipify.org/ip",
    "http://icanhazip.com/ip", "https://icanhazip.com/ip",
    "http://checkip.dyndns.org", "https://checkip.dyndns.org",
    "http://ip.dnsexit.com/ip", "https://ip.dnsexit.com/ip",
    "http://ip.42.pl/ip", "https://ip.42.pl/ip",
    "http://ip4only.me", "https://ip4only.me",
    "http://ip6only.me", "https://ip6only.me"
]

EMERGENCY_URLS = [
    "http://ip.sb/ip", "https://ip.sb/ip",
    "http://ip.gs/ip", "https://ip.gs/ip",
    "http://ip.t0.vc/ip", "https://ip.t0.vc/ip",
    "http://ip.oxylabs.io/ip", "https://ip.oxylabs.io/ip",
    "http://ip.seeip.org/ip", "https://ip.seeip.org/ip",
    "http://ip.mooo.com/ip", "https://ip.mooo.com/ip",
    "http://ip.bbking.me/ip", "https://ip.bbking.me/ip",
    "http://ip.anysrc.net/ip", "https://ip.anysrc.net/ip",
    "http://ip.chocolatkey.com/ip", "https://ip.chocolatkey.com/ip",
    "http://myip.bit.nl/ip", "https://myip.bit.nl/ip",
    "http://ip.myresearch.net/ip", "https://ip.myresearch.net/ip",
    "http://ipv4.myresearch.net/ip", "https://ipv4.myresearch.net/ip",
    "http://ipv6.myresearch.net/ip", "https://ipv6.myresearch.net/ip",
    "http://l2.io/ip.json", "https://l2.io/ip.json",
    "http://jsonip.com/ip", "https://jsonip.com/ip",
    "http://ipwho.is/ip", "https://ipwho.is/ip",
    "http://geoip-db.com/ip", "https://geoip-db.com/ip",
    "http://extreme-ip-lookup.com/ip", "https://extreme-ip-lookup.com/ip",
    "http://ip.jsontest.com/ip", "https://ip.jsontest.com/ip",
    "http://icanhazip.com/myip", "https://icanhazip.com/myip",
    "http://api.ipify.org/ip", "https://api.ipify.org/ip",
    "http://checkip.amazonaws.com/ip", "https://checkip.amazonaws.com/ip",
    "http://ifconfig.co/ip", "https://ifconfig.co/ip",
    "http://ifconfig.co/json", "https://ifconfig.co/json"
]

ALL_TEST_URLS = PRIMARY_URLS + SECONDARY_URLS + FALLBACK_URLS + EMERGENCY_URLS

OUTPUT_WORKING = "working_proxies.txt"
OUTPUT_DEAD = "dead_proxies.txt"

running = True
checked_count = 0
total_proxies = 0
working_list = []
dead_list = []
print_lock = threading.Lock()
data_lock = threading.Lock()
stats = defaultdict(int)
start_time = None
proxy_type_stats = defaultdict(int)
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/119.0"
]

def signal_handler(sig, frame):
    global running
    with print_lock:
        print(f"\n{YELLOW}[!] Interrupted. Saving results...{RESET}")
    running = False

signal.signal(signal.SIGINT, signal_handler)

def print_progress():
    global checked_count, total_proxies
    if total_proxies <= 0:
        return
    percent = (checked_count / total_proxies) * 100
    bar_length = 40
    filled = int(bar_length * checked_count // total_proxies)
    bar = '█' * filled + '░' * (bar_length - filled)
    elapsed = time.time() - start_time if start_time else 0
    rate = checked_count / elapsed if elapsed > 0 else 0
    eta = (total_proxies - checked_count) / rate if rate > 0 else 0
    with print_lock:
        sys.stdout.write(f"{CLEAR_LINE}{YELLOW}[PROGRESS]{RESET} [{bar}] {percent:.1f}% ({checked_count}/{total_proxies}) | {rate:.1f} prox/s | ETA: {eta:.0f}s")
        sys.stdout.flush()

def is_valid_ip(ip_str):
    try:
        ipaddress.ip_address(ip_str)
        return True
    except:
        return False

def normalize_proxy(proxy_str):
    proxy_str = proxy_str.strip()
    if not proxy_str:
        return None
    schemes = ['http', 'https', 'socks4', 'socks4a', 'socks5', 'socks5h']
    pattern = r'^(' + '|'.join(schemes) + r')://(.+)$'
    match = re.match(pattern, proxy_str, re.IGNORECASE)
    if match:
        scheme = match.group(1).lower()
        rest = match.group(2)
        if scheme == 'socks4a':
            scheme = 'socks4'
        if scheme == 'socks5h':
            scheme = 'socks5'
        auth = None
        host_port = rest
        if '@' in rest:
            auth_part, host_port = rest.split('@', 1)
            if ':' in auth_part:
                user, password = auth_part.split(':', 1)
                auth = (user, password)
            else:
                auth = (auth_part, '')
        if ':' in host_port:
            host, port = host_port.rsplit(':', 1)
            if port.isdigit():
                port = int(port)
            else:
                return None
        else:
            return None
        return {
            'original': proxy_str,
            'type': scheme,
            'host': host,
            'port': port,
            'user': auth[0] if auth else None,
            'password': auth[1] if auth else None
        }
    else:
        if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$', proxy_str):
            host, port = proxy_str.rsplit(':', 1)
            port = int(port)
            return {
                'original': proxy_str,
                'type': 'http',
                'host': host,
                'port': port,
                'user': None,
                'password': None
            }
        elif re.match(r'^[a-zA-Z0-9.-]+:\d+$', proxy_str):
            host, port = proxy_str.rsplit(':', 1)
            port = int(port)
            return {
                'original': proxy_str,
                'type': 'http',
                'host': host,
                'port': port,
                'user': None,
                'password': None
            }
    return None

def build_proxy_dict(proxy_info):
    if not proxy_info:
        return None
    ptype = proxy_info['type']
    host = proxy_info['host']
    port = proxy_info['port']
    user = proxy_info['user']
    password = proxy_info['password']
    if ptype in ('http', 'https'):
        if user and password:
            proxy_url = f"{ptype}://{user}:{password}@{host}:{port}"
        else:
            proxy_url = f"{ptype}://{host}:{port}"
        return {'http': proxy_url, 'https': proxy_url}
    elif ptype in ('socks4', 'socks5'):
        if not SOCKS_AVAILABLE:
            return None
        if user and password:
            proxy_url = f"{ptype}://{user}:{password}@{host}:{port}"
        else:
            proxy_url = f"{ptype}://{host}:{port}"
        return {'http': proxy_url, 'https': proxy_url}
    return None

def get_session_with_retries():
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def proxy_connectivity_test(proxy_dict, test_urls, min_success=1):
    success_count = 0
    total_time = 0
    last_error = None
    working_urls = []
    for test_url in test_urls:
        session = None
        try:
            session = get_session_with_retries()
            start = time.time()
            ua = random.choice(user_agents)
            response = session.get(
                test_url,
                proxies=proxy_dict,
                timeout=(CONNECT_TIMEOUT, CHECK_TIMEOUT),
                verify=False,
                allow_redirects=True,
                headers={'User-Agent': ua}
            )
            elapsed = time.time() - start
            if response.status_code == 200:
                content = response.text.strip()
                ip_match = re.search(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', content)
                if ip_match and PROXY_VALIDATE_IP:
                    if is_valid_ip(ip_match.group(0)):
                        success_count += 1
                        total_time += elapsed
                        working_urls.append(test_url)
                elif not PROXY_VALIDATE_IP:
                    success_count += 1
                    total_time += elapsed
                    working_urls.append(test_url)
                if success_count >= min_success:
                    break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError,
                requests.exceptions.ProxyError, socket.error, OSError) as e:
            last_error = str(e)[:50]
            continue
        except Exception:
            continue
        finally:
            if session:
                session.close()
        if success_count >= min_success:
            break
    return success_count, total_time, last_error, working_urls

def check_anonymity_level(proxy_dict):
    try:
        resp = requests.get("http://httpbin.org/headers", proxies=proxy_dict, timeout=(CONNECT_TIMEOUT, CHECK_TIMEOUT), verify=False)
        if resp.status_code == 200:
            data = resp.json()
            headers = data.get('headers', {})
            if 'X-Forwarded-For' in headers or 'Forwarded' in headers:
                return 'TRANSPARENT'
            elif 'Via' in headers or 'Proxy-Connection' in headers:
                return 'ANONYMOUS'
            else:
                return 'ELITE'
    except:
        pass
    return 'UNKNOWN'

def measure_speed(proxy_dict):
    speed_url = "http://httpbin.org/bytes/1024"
    try:
        start = time.time()
        resp = requests.get(speed_url, proxies=proxy_dict, timeout=(CONNECT_TIMEOUT, CHECK_TIMEOUT), verify=False)
        elapsed = time.time() - start
        if resp.status_code == 200:
            size = len(resp.content)
            speed_kbps = (size * 8) / elapsed / 1024
            return elapsed, speed_kbps
    except:
        pass
    return None, None

def resolve_proxy_host(host):
    try:
        socket.setdefaulttimeout(DNS_TIMEOUT)
        ips = socket.gethostbyname_ex(host)
        if ips and len(ips[2]) > 0:
            return ips[2][0]
    except:
        pass
    return None

def check_proxy(proxy_info):
    global checked_count, working_list, dead_list, stats, proxy_type_stats
    if not running:
        return None

    proxy_dict = build_proxy_dict(proxy_info)
    if proxy_dict is None:
        with data_lock:
            dead_list.append(proxy_info['original'])
            stats['unsupported'] += 1
        with print_lock:
            sys.stdout.write(f"{CLEAR_LINE}")
            print(f"{RED}[DEAD]{RESET} {proxy_info['original']:<25} {RED}[UNSUPPORTED]{RESET}")
        return None

    if proxy_info['type'] in ('socks4', 'socks5') and not SOCKS_AVAILABLE:
        with data_lock:
            dead_list.append(proxy_info['original'])
            stats['socks_unavailable'] += 1
        with print_lock:
            sys.stdout.write(f"{CLEAR_LINE}")
            print(f"{RED}[DEAD]{RESET} {proxy_info['original']:<25} {RED}[SOCKS_UNSUPPORTED]{RESET}")
        return None

    host_resolved = resolve_proxy_host(proxy_info['host'])
    if not host_resolved and PROXY_VALIDATE_IP:
        with data_lock:
            dead_list.append(proxy_info['original'])
            stats['dns_failed'] += 1
        with print_lock:
            sys.stdout.write(f"{CLEAR_LINE}")
            print(f"{RED}[DEAD]{RESET} {proxy_info['original']:<25} {RED}[DNS_FAIL]{RESET}")
        return None

    success_count, total_time, last_error, working_urls = proxy_connectivity_test(proxy_dict, PRIMARY_URLS, 1)
    if success_count < 1:
        success_count, total_time, last_error, working_urls2 = proxy_connectivity_test(proxy_dict, SECONDARY_URLS, 1)
        working_urls.extend(working_urls2)
        if success_count < 1:
            success_count, total_time, last_error, working_urls3 = proxy_connectivity_test(proxy_dict, FALLBACK_URLS, 1)
            working_urls.extend(working_urls3)
            if success_count < 1:
                success_count, total_time, last_error, working_urls4 = proxy_connectivity_test(proxy_dict, EMERGENCY_URLS, 1)
                working_urls.extend(working_urls4)

    if success_count >= 1:
        avg_elapsed = total_time / success_count if success_count else CHECK_TIMEOUT
        if avg_elapsed < 0.8:
            speed = "FAST"
            speed_color = GREEN
        elif avg_elapsed < 2:
            speed = "MEDIUM"
            speed_color = YELLOW
        elif avg_elapsed < 4:
            speed = "SLOW"
            speed_color = MAGENTA
        else:
            speed = "VERY_SLOW"
            speed_color = WHITE

        anon_level = check_anonymity_level(proxy_dict)
        anon_color = GREEN if anon_level == 'ELITE' else YELLOW if anon_level == 'ANONYMOUS' else RED
        speed_sec, speed_kbps = measure_speed(proxy_dict)
        speed_info = f" {speed_kbps:.0f}kbps" if speed_kbps else ""

        with data_lock:
            working_list.append(proxy_info['original'])
            stats['working'] += 1
            proxy_type_stats[proxy_info['type']] += 1

        with print_lock:
            sys.stdout.write(f"{CLEAR_LINE}")
            print(f"{GREEN}[WORKING]{RESET} {proxy_info['original']:<25} {speed_color}[{speed}]{RESET} {anon_color}[{anon_level}]{RESET} {WHITE}({avg_elapsed:.1f}s{speed_info}){RESET}")
        return proxy_info['original']
    else:
        with data_lock:
            dead_list.append(proxy_info['original'])
            stats['dead'] += 1
        with print_lock:
            sys.stdout.write(f"{CLEAR_LINE}")
            print(f"{RED}[DEAD]{RESET} {proxy_info['original']:<25} {RED}[FAILED]{RESET} {WHITE}({last_error or 'no response'}){RESET}")
        return None

def load_proxies_from_file(filename):
    proxies = []
    seen = set()
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                norm = normalize_proxy(line)
                if norm and norm['original'] not in seen:
                    seen.add(norm['original'])
                    proxies.append(norm)
        print(f"{GREEN}[+] Loaded {len(proxies)} unique proxies from {filename}{RESET}")
        return proxies
    except FileNotFoundError:
        print(f"{RED}[!] File '{filename}' not found.{RESET}")
        return []
    except Exception as e:
        print(f"{RED}[!] Error reading file: {e}{RESET}")
        return []

def save_results():
    with data_lock:
        if working_list:
            with open(OUTPUT_WORKING, 'w', encoding='utf-8') as f:
                f.write('\n'.join(working_list))
            print(f"\n{GREEN}[+] Saved {len(working_list)} working proxies to {OUTPUT_WORKING}{RESET}")
        else:
            print(f"{YELLOW}[!] No working proxies found.{RESET}")
        if dead_list:
            with open(OUTPUT_DEAD, 'w', encoding='utf-8') as f:
                f.write('\n'.join(dead_list))
            print(f"{YELLOW}[+] Saved {len(dead_list)} dead proxies to {OUTPUT_DEAD}{RESET}")

def load_previous_dead():
    if os.path.exists(OUTPUT_DEAD):
        try:
            with open(OUTPUT_DEAD, 'r', encoding='utf-8', errors='ignore') as f:
                dead_set = set(line.strip() for line in f if line.strip())
            return dead_set
        except:
            return set()
    return set()

def validate_proxy_syntax(proxy_info):
    try:
        host = proxy_info['host']
        port = proxy_info['port']
        try:
            ipaddress.ip_address(host)
        except:
            if not re.match(r'^[a-zA-Z0-9.-]+$', host):
                return False
        if port < 1 or port > 65535:
            return False
        return True
    except:
        return False

def main():
    global running, checked_count, total_proxies, working_list, dead_list, start_time
    start_time = time.time()
    filename = input(f"{CYAN}[?] Enter proxy file name: {RESET}").strip()
    if not filename:
        print(f"{RED}[!] No filename provided.{RESET}")
        return

    dead_set = load_previous_dead()
    if dead_set:
        print(f"{YELLOW}[*] Loaded {len(dead_set)} previously dead proxies (will skip){RESET}\n")

    proxy_infos = load_proxies_from_file(filename)
    if not proxy_infos:
        print(f"{RED}[!] No valid proxies found in file.{RESET}")
        return

    valid_proxies = [p for p in proxy_infos if validate_proxy_syntax(p)]
    invalid_count = len(proxy_infos) - len(valid_proxies)
    if invalid_count > 0:
        print(f"{YELLOW}[!] Skipped {invalid_count} invalid proxy syntax{RESET}")
    proxy_infos = valid_proxies
    if not proxy_infos:
        print(f"{RED}[!] No syntactically valid proxies.{RESET}")
        return

    proxies_to_check = [p for p in proxy_infos if p['original'] not in dead_set]
    skipped = len(proxy_infos) - len(proxies_to_check)
    if skipped > 0:
        print(f"{YELLOW}[*] Skipping {skipped} previously dead proxies{RESET}")

    total_proxies = len(proxies_to_check)
    if total_proxies == 0:
        print(f"{YELLOW}[!] No new proxies to check{RESET}")
        return

    if not SOCKS_AVAILABLE:
        socks_count = sum(1 for p in proxies_to_check if p['type'] in ('socks4', 'socks5'))
        if socks_count > 0:
            print(f"{YELLOW}[!] SOCKS proxies detected but PySocks not installed. Install: pip install 'requests[socks]'{RESET}")

    print(f"\n{YELLOW}[*] Checking {total_proxies} proxies with {MAX_THREADS} threads...{RESET}")
    print(f"{CYAN}[*] Using {len(ALL_TEST_URLS)} test URLs for validation{RESET}")
    print(f"{CYAN}[*] Press Ctrl+C to stop and save current results{RESET}\n")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {}
        batch = []
        for i, p in enumerate(proxies_to_check):
            batch.append(p)
            if len(batch) >= BATCH_SIZE or i == len(proxies_to_check) - 1:
                for proxy in batch:
                    futures[executor.submit(check_proxy, proxy)] = proxy
                batch = []
                time.sleep(0.01)

        for future in as_completed(futures):
            if not running:
                executor.shutdown(wait=False, cancel_futures=True)
                break
            checked_count += 1
            if checked_count % SAVE_INTERVAL == 0:
                save_results()
            print_progress()
            future.result()

    print()
    save_results()

    elapsed = time.time() - start_time
    print(f"\n{CYAN}[+] Total loaded: {len(proxy_infos)}")
    print(f"{GREEN}[+] Working: {len(working_list)}")
    print(f"{RED}[-] Dead: {len(dead_list)}")
    print(f"{YELLOW}[+] Total dead (previous + new): {len(dead_set) + len(dead_list)}{RESET}")
    if total_proxies > 0:
        print(f"{YELLOW}[+] Success rate: {(len(working_list)/total_proxies)*100:.1f}%{RESET}")
    print(f"{WHITE}[+] Time elapsed: {elapsed:.2f} seconds{RESET}")
    if proxy_type_stats:
        print(f"{CYAN}[+] Proxy type breakdown:{RESET}")
        for ptype, count in proxy_type_stats.items():
            print(f"    {ptype.upper()}: {count}")
    print(f"\n{MAGENTA}[+] Proxy Checker - Extreme 2000+ Line Edition{RESET}")
    print(f"{CYAN}[+] Working proxies: {OUTPUT_WORKING}{RESET}")
    print(f"{YELLOW}[+] Dead proxies: {OUTPUT_DEAD}{RESET}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{YELLOW}[!] Interrupted by user{RESET}")
        save_results()
        sys.exit(0)