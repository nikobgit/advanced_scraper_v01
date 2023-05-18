from urllib.parse import urlparse

def is_same_domain(url, main_domain):
    parsed_main_domain = urlparse(main_domain)
    main_domain_parts = parsed_main_domain.hostname.split('.')
    main_root_domain = '.'.join(main_domain_parts[-2:])

    parsed_url = urlparse(url)
    url_domain_parts = parsed_url.hostname.split('.')
    url_root_domain = '.'.join(url_domain_parts[-2:])

    return main_root_domain == url_root_domain or main_domain.endswith(f".{url_root_domain}")
