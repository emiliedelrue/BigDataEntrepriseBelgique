"""
tor_session.py
--------------
Gestion des sessions HTTP via les proxies Tor.
Rotation automatique sur erreur 429 / timeout.
"""

import logging
import random
import time

import requests

logger = logging.getLogger(__name__)

# ── Proxies disponibles (noms de services Docker) ─────────────────────────────
TOR_PROXIES = [
    "socks5h://tor1:9050",
    "socks5h://tor2:9050",
    "socks5h://tor3:9050",
]

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) "
        "Gecko/20100101 Firefox/128.0"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


def make_session(proxy_url: str | None = None) -> requests.Session:
    """Crée une Session requests avec headers standard et proxy Tor optionnel."""
    session = requests.Session()
    session.headers.update(_DEFAULT_HEADERS)
    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}
    return session


def get_with_rotation(
    url: str,
    params: dict | None = None,
    extra_headers: dict | None = None,
    max_retries: int = 9,
    base_wait: float = 2.0,
    timeout: int = 30,
    init_session_url: str | None = None,
) -> requests.Response:
    """
    GET avec rotation Tor automatique.

    Stratégie :
      - On mélange aléatoirement les proxies disponibles.
      - Sur 429 / 503 : attente exponentielle + changement de proxy.
      - Sur timeout / connexion : changement de proxy immédiat.
      - max_retries répartis cycliquement sur les proxies.

    Parameters
    ----------
    url             : URL cible
    params          : query params
    extra_headers   : headers supplémentaires (ex. Referer)
    max_retries     : nombre total de tentatives
    base_wait       : délai de base avant retry (doublé à chaque tentative)
    timeout         : timeout par requête en secondes
    init_session_url: si fournie, la session visite cette URL d'abord
                      pour récupérer les cookies (ex. page d'accueil CBSO)

    Returns
    -------
    requests.Response avec status_code 200

    Raises
    ------
    RuntimeError si toutes les tentatives échouent
    """
    proxies = random.sample(TOR_PROXIES, len(TOR_PROXIES))
    wait = base_wait

    for attempt in range(max_retries):
        proxy = proxies[attempt % len(proxies)]
        session = make_session(proxy)

        if extra_headers:
            session.headers.update(extra_headers)

        # Warm-up : visite de la page d'accueil pour obtenir les cookies
        if init_session_url:
            try:
                session.get(init_session_url, timeout=15)
            except Exception:
                pass  # Non bloquant

        try:
            logger.info(f"[TOR] Tentative {attempt + 1}/{max_retries} — proxy={proxy}")
            resp = session.get(url, params=params, timeout=timeout)

            if resp.status_code == 200:
                logger.info(f"[TOR] Succès ({resp.status_code}) en {attempt + 1} tentative(s)")
                return resp

            elif resp.status_code in (429, 503):
                logger.warning(
                    f"[TOR] Rate-limit {resp.status_code} — attente {wait:.1f}s "
                    f"puis changement de proxy"
                )
                time.sleep(wait + random.uniform(0, 1))
                wait = min(wait * 2, 60)

            elif resp.status_code in (403, 404, 500):
                # 403/404 : accès refusé ou ressource inexistante
                # 500 : erreur serveur CBSO = fichier indisponible dans ce format
                # → inutile de retenter, on perd du temps
                logger.error(f"[TOR] {resp.status_code} pour {url} — abandon immédiat")
                return resp

            else:
                logger.warning(f"[TOR] Statut inattendu {resp.status_code}")
                time.sleep(min(wait, 10))   # cap à 10s max pour les autres codes
                wait = min(wait * 1.5, 30)

        except (requests.exceptions.ProxyError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            logger.warning(f"[TOR] Erreur réseau ({exc.__class__.__name__}) — changement de proxy")
            time.sleep(2)

    raise RuntimeError(
        f"[TOR] Échec après {max_retries} tentatives pour {url}"
    )


def post_with_rotation(
    url: str,
    json: dict | None = None,
    data: dict | None = None,
    extra_headers: dict | None = None,
    max_retries: int = 6,
    timeout: int = 30,
) -> requests.Response:
    """POST avec rotation Tor (même logique que get_with_rotation)."""
    proxies = random.sample(TOR_PROXIES, len(TOR_PROXIES))
    wait = 2.0

    for attempt in range(max_retries):
        proxy = proxies[attempt % len(proxies)]
        session = make_session(proxy)
        if extra_headers:
            session.headers.update(extra_headers)

        try:
            resp = session.post(url, json=json, data=data, timeout=timeout)
            if resp.status_code == 200:
                return resp
            elif resp.status_code in (429, 503):
                time.sleep(wait + random.uniform(0, 1))
                wait = min(wait * 2, 60)
            else:
                return resp
        except Exception as exc:
            logger.warning(f"[TOR] POST erreur: {exc}")
            time.sleep(2)

    raise RuntimeError(f"[TOR] POST échoué après {max_retries} tentatives")