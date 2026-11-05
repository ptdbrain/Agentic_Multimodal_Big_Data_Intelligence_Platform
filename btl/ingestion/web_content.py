"""Independent article parsing; web posts are never fabricated product reviews."""
import hashlib
from urllib.parse import urlsplit, urlunsplit
from bs4 import BeautifulSoup
from ingestion.crawler.generic_html import _documents


def parse_article(html: str, url: str, collected_at: str) -> dict:
    parts = urlsplit(url)
    if parts.scheme not in ('http', 'https') or not parts.hostname:
        raise ValueError('Absolute HTTP article URL required')
    canonical = urlunsplit((parts.scheme, parts.netloc.lower(), parts.path, '', ''))
    soup = BeautifulSoup(html, 'html.parser')
    article = soup.find('article')
    heading = article.find('h1') if article else None
    headline, published = None, None
    for item in _documents(html):
        types = item.get('@type', [])
        types = [types] if isinstance(types, str) else types
        if isinstance(types, list) and {'Article', 'NewsArticle', 'DiscussionForumPosting', 'BlogPosting'}.intersection(types):
            headline = item.get('headline')
            published = item.get('datePublished')
            break
    headline = headline or (heading.get_text(' ', strip=True) if heading else None)
    if article is None or not headline:
        raise ValueError('No observed article body/headline')
    for element in article.find_all(['script', 'style', 'nav', 'form']):
        element.decompose()
    body = article.get_text(' ', strip=True)
    if not body:
        raise ValueError('Empty article body')
    return {
        'article_id': 'article_' + hashlib.sha256(canonical.encode()).hexdigest()[:24],
        'source_record_id':canonical,'parser_version':'article-html-v2','schema_version':'1.0',
        'source_group': 'web_content', 'source_name': parts.hostname.lower(),
        'source_url': canonical, 'acquisition_method': 'live',
        'title': str(headline), 'body': body,
        'published_at': published, 'collected_at': collected_at,
        'content_sha256': hashlib.sha256(body.encode()).hexdigest(),
    }


def collect_articles(seed_urls, limit=50, max_pages=150, evidence_dir=None, request_interval=0.5, collection_run_id=None):
    """Bounded, polite BFS of public thread links; never synthesize missing posts.

    Original responses are optional private evidence; callers publish the
    metadata manifest, not HTML/cookies. A shortage remains visible in counts.
    """
    from collections import deque
    from datetime import datetime, timezone
    from pathlib import Path
    from urllib.parse import urljoin
    from urllib.robotparser import RobotFileParser
    import requests
    import time
    if limit < 1 or max_pages < 1 or request_interval < 0:
        raise ValueError('Invalid collection bounds')
    import uuid
    collection_run_id = collection_run_id or uuid.uuid4().hex
    seeds = list(seed_urls)
    if not seeds or any(urlsplit(url).scheme not in ('http', 'https') for url in seeds):
        raise ValueError('Public HTTP seed URLs required')
    allowed_hosts = {urlsplit(url).netloc for url in seeds}
    agent = 'SentinelAI-Academic-Research/1.0'
    queue, seen, policies, records, evidence = deque(seeds), set(), {}, [], []
    output = Path(evidence_dir) if evidence_dir is not None else None
    if output:
        output.mkdir(parents=True, exist_ok=True, mode=0o700)
    with requests.Session() as session:
        session.headers['User-Agent'] = agent
        while queue and len(seen) < max_pages and len(records) < limit:
            url = queue.popleft().split('#')[0].split('?')[0]
            parts = urlsplit(url)
            if url in seen or parts.netloc not in allowed_hosts:
                continue
            seen.add(url)
            info = {'url': url, 'collected_at': datetime.now(timezone.utc).isoformat()}
            try:
                if parts.netloc not in policies:
                    robots = session.get(urlunsplit((parts.scheme, parts.netloc, '/robots.txt', '', '')), timeout=20)
                    robots.raise_for_status()
                    parser = RobotFileParser(); parser.parse(robots.text.splitlines())
                    policies[parts.netloc] = parser
                if not policies[parts.netloc].can_fetch(agent, url):
                    info['error'] = 'robots_disallowed'
                    continue
                time.sleep(request_interval)
                response = session.get(url, timeout=20)
                info['http_status'] = response.status_code
                response.raise_for_status()
                if urlsplit(response.url).netloc not in allowed_hosts:
                    raise ValueError('Redirect outside selected source')
                content = response.content
                if len(content) > 5 * 1024 * 1024:
                    raise ValueError('Response exceeds bounded article parser size')
                digest = hashlib.sha256(content).hexdigest()
                info['response_sha256'] = digest
                if output:
                    path = output / (digest + '.html')
                    path.write_bytes(content); path.chmod(0o600)
                    info['evidence_file'] = str(path)
                soup = BeautifulSoup(response.text, 'html.parser')
                for link in soup.select('a[href]'):
                    target = urljoin(response.url, link['href']).split('#')[0].split('?')[0]
                    if '/thread/' in target and urlsplit(target).netloc in allowed_hosts and target not in seen:
                        queue.append(target)
                if '/thread/' in parts.path:
                    row = parse_article(response.text, response.url, info['collected_at'])
                    row['evidence_sha256'] = digest
                    row['collection_run_id'] = collection_run_id
                    if output:
                        row['evidence_ref'] = str(path.resolve())
                    if row['article_id'] not in {item['article_id'] for item in records}:
                        records.append(row)
                        info['article_id'] = row['article_id']
            except (requests.RequestException, ValueError) as error:
                info['error'] = type(error).__name__
            finally:
                evidence.append(info)
    return records, {'collection_run_id':collection_run_id,'acquisition_method': 'live', 'articles_collected': len(records), 'requested_articles': limit,
                     'pages_attempted': len(seen), 'requests': evidence}
